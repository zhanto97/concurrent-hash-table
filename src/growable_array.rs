use core::fmt::Debug;
use core::marker::PhantomData;
use core::mem;
use core::ops::{Deref, DerefMut};
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{unprotected, Atomic, Guard, Owned, Pointer, Shared};

/// Growable array of `Atomic<T>`.
///
/// This is more complete version of the dynamic sized array from the paper. In the paper, the
/// segment table is an array of arrays (segments) of pointers to the elements. In this
/// implementation, a segment contains the pointers to the elements **or other segments**. In other
/// words, it is a tree that has segments as internal nodes.
///
/// # Example run
///
/// Suppose `SEGMENT_LOGSIZE = 3` (segment size 8).
///
/// When a new `GrowableArray` is created, `root` is initialized with `Atomic::null()`.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
/// ```
///
/// When you store element `cat` at the index `0b001`, it first initializes a segment.
///
/// ```text
///
///                          +----+
///                          |root|
///                          +----+
///                            | height: 1
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                           |
///                                           v
///                                         +---+
///                                         |cat|
///                                         +---+
/// ```
///
/// When you store `fox` at `0b111011`, it is clear that there is no room for indices larger than
/// `0b111`. So it first allocates another segment for upper 3 bits and moves the previous root
/// segment (`0b000XXX` segment) under the `0b000XXX` branch of the the newly allocated segment.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                                               |
///                                               v
///                                      +---+---+---+---+---+---+---+---+
///                                      |111|110|101|100|011|010|001|000|
///                                      +---+---+---+---+---+---+---+---+
///                                                                |
///                                                                v
///                                                              +---+
///                                                              |cat|
///                                                              +---+
/// ```
///
/// And then, it allocates another segment for `0b111XXX` indices.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                                            |
///                   v                                            v
///                 +---+                                        +---+
///                 |fox|                                        |cat|
///                 +---+                                        +---+
/// ```
///
/// Finally, when you store `owl` at `0b000110`, it traverses through the `0b000XXX` branch of the
/// level-1 segment and arrives at its 0b110` leaf.
///
/// ```text
///                          +----+
///                          |root|
///                          +----+
///                            | height: 2
///                            v
///                 +---+---+---+---+---+---+---+---+
///                 |111|110|101|100|011|010|001|000|
///                 +---+---+---+---+---+---+---+---+
///                   |                           |
///                   v                           v
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
/// |111|110|101|100|011|010|001|000|    |111|110|101|100|011|010|001|000|
/// +---+---+---+---+---+---+---+---+    +---+---+---+---+---+---+---+---+
///                   |                        |                   |
///                   v                        v                   v
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// When the array is dropped, only the segments are dropped and the **elements must not be
/// dropped/deallocated**.
///
/// ```test
///                 +---+                    +---+               +---+
///                 |fox|                    |owl|               |cat|
///                 +---+                    +---+               +---+
/// ```
///
/// Instead, it should be handled by the container that the elements actually belong to. For
/// example in `SplitOrderedList`, destruction of elements are handled by `List`.
///
#[derive(Debug)]
pub struct GrowableArray<T> {
    root: Atomic<Segment>,
    _marker: PhantomData<T>,
}

const SEGMENT_LOGSIZE: usize = 10;

struct Segment {
    /// `AtomicUsize` here means `Atomic<T>` or `Atomic<Segment>`.
    inner: [AtomicUsize; 1 << SEGMENT_LOGSIZE],
}

impl Segment {
    fn new() -> Self {
        Self {
            inner: unsafe { mem::zeroed() },
        }
    }

    fn get_unchecked(&self, index: usize) -> &AtomicUsize {
        return &self.inner[index];
    }
}

impl Deref for Segment {
    type Target = [AtomicUsize; 1 << SEGMENT_LOGSIZE];

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl DerefMut for Segment {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl Debug for Segment {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Segment")
    }
}

impl<T> Drop for GrowableArray<T> {
    /// Deallocate segments, but not the individual elements.
    fn drop(&mut self) {
        unsafe {
            let guard = unprotected();

            let root = self.root.load(Ordering::Acquire, guard);
            if root.is_null() {
                return;
            }

            let root_height = root.tag();
            let owned = root.into_owned();
            self._drop(owned, root_height, guard);
        }
    }
}

impl<T> Default for GrowableArray<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GrowableArray<T> {
    /// Create a new growable array.
    pub fn new() -> Self {
        Self {
            root: Atomic::null(),
            _marker: PhantomData,
        }
    }

    fn _drop(&mut self, owned: Owned<Segment>, height: usize, guard: &Guard) {
        // Drop segments by DFS traversal

        if height == 1 {
            drop(owned);
            return;
        }

        for i in 0..(1 << SEGMENT_LOGSIZE) {
            let child =
                unsafe { &*((*owned).get_unchecked(i) as *const _ as *const Atomic<Segment>) };
            let temp = (*child).load(Ordering::Acquire, guard);
            if !temp.is_null() {
                self._drop(unsafe { temp.into_owned() }, height - 1, guard);
            }
        }
        drop(owned);
    }

    fn get_bits_at(&self, index: usize, mut mask: usize, at: usize) -> usize {
        // bits of INDEX can be partitioned into segments each having SEGMENT_LOGSIZE bits
        // returns SEGMENT_LOGSIZE bits located at AT segment
        // Ex: at = 0 returns SEGMENT_LOGSIZE lsb of INDEX
        // Ex: at = 1 return next SEGMENT_LOGSIZE lsb of INDEX

        mask = mask << (at * SEGMENT_LOGSIZE);
        let mut bits: usize = index & mask;
        bits = bits >> (at * SEGMENT_LOGSIZE);
        return bits;
    }

    fn get_msb_index(&self, index: usize) -> usize {
        let zeros = index.leading_zeros() as usize;
        let size = mem::size_of::<usize>() * 8;
        return size - zeros;
    }

    fn ensure_root_height(&self, height: usize, guard: &Guard) {
        // Ensures that root of GrowableArray has height at least HEIGHT
        // by creating new segments at root if necessary

        loop {
            let root = self.root.load(Ordering::Acquire, guard);
            let root_height = root.tag();
            if root_height < height {
                let new_seg = Segment::new();
                new_seg.inner[0].store(root.into_usize(), Ordering::Release);

                let new_root_height = root_height + 1;
                let new_root = Owned::new(new_seg);
                self.root.compare_and_set(
                    root,
                    new_root.with_tag(new_root_height),
                    Ordering::AcqRel,
                    guard,
                );
            } else {
                break;
            }
        }
    }

    fn get_val_at_index(&self, index: usize, guard: &Guard) -> &Atomic<T> {
        // Goes down the segments to get pointer value stored at INDEX
        // Initializes child segments if necessary

        let mut reference = &self.root;
        loop {
            let root = (*reference).load(Ordering::Acquire, guard);
            let root_height = root.tag();

            let mask: usize = (1 << SEGMENT_LOGSIZE) - 1;
            let ind = self.get_bits_at(index, mask, root_height - 1);

            if root_height == 1 {
                return unsafe {
                    &*(root.deref().get_unchecked(ind) as *const _ as *const Atomic<T>)
                };
            }

            reference = unsafe {
                &*(root.deref().get_unchecked(ind) as *const _ as *const Atomic<Segment>)
            };
            let temp = (*reference).load(Ordering::Acquire, guard);
            if temp.is_null() {
                let new_child_height = root_height - 1;
                let new_child = Owned::new(Segment::new());
                (*reference).compare_and_set(
                    temp,
                    new_child.with_tag(new_child_height),
                    Ordering::AcqRel,
                    guard,
                );
            }
        }
    }

    /// Returns the reference to the `Atomic` pointer at `index`. Allocates new segments if
    /// necessary.
    pub fn get(&self, index: usize, guard: &Guard) -> &Atomic<T> {
        let msb = self.get_msb_index(index);

        let root = self.root.load(Ordering::Acquire, guard);
        if root.is_null() {
            let new_root_height = 1;
            let new_root = Owned::new(Segment::new());
            self.root.compare_and_set(
                root,
                new_root.with_tag(new_root_height),
                Ordering::AcqRel,
                guard,
            );
        }

        if msb % SEGMENT_LOGSIZE == 0 {
            self.ensure_root_height(msb / SEGMENT_LOGSIZE, guard);
        } else {
            self.ensure_root_height(msb / SEGMENT_LOGSIZE + 1, guard);
        }
        return self.get_val_at_index(index, guard);
    }
}
