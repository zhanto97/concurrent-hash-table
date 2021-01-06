//! Split-ordered linked list.

use core::mem;
use core::sync::atomic::{AtomicUsize, Ordering};
use crossbeam_epoch::{Guard, Owned};
use lockfree::list::{Cursor, List, Node};
// use cs492_concur_homework::map::NonblockingMap;

use super::growable_array::GrowableArray;

/// Lock-free map from `usize` in range [0, 2^63-1] to `V`.
///
/// NOTE: We don't care about hashing in this homework for simplicity.
#[derive(Debug)]
pub struct SplitOrderedList<V> {
    /// Lock-free list sorted by recursive-split order. Use `None` sentinel node value.
    list: List<usize, Option<V>>,
    /// array of pointers to the buckets
    buckets: GrowableArray<Node<usize, Option<V>>>,
    /// number of buckets
    size: AtomicUsize,
    /// number of items
    count: AtomicUsize,
}

impl<V> Default for SplitOrderedList<V> {
    fn default() -> Self {
        Self {
            list: List::new(),
            buckets: GrowableArray::new(),
            size: AtomicUsize::new(2),
            count: AtomicUsize::new(0),
        }
    }
}

impl<V> SplitOrderedList<V> {
    /// `size` is doubled when `count > size * LOAD_FACTOR`.
    const LOAD_FACTOR: usize = 2;
    const HI_MASK: usize = 0x8000000000000000; //1 << (mem::size_of::<usize>()*8 - 1)

    /// Creates a new split ordered list.
    pub fn new() -> Self {
        Self::default()
    }

    fn get_parent(&self, bucket_index: usize) -> usize {
        let mut parent: usize = self.size.load(Ordering::Acquire);
        loop {
            parent = parent >> 1;
            if (parent <= bucket_index) {
                break;
            }
        }
        return bucket_index - parent;
    }

    fn make_sentinel<'s>(&self, parent_index: usize, child_index: usize, guard: &'s Guard) {
        let key = child_index.reverse_bits();
        let mut owned = Owned::new(Node::new(key, None));
        let parent = self.buckets.get(parent_index, guard);
        loop {
            let mut cursor =
                unsafe { Cursor::from_raw(parent, parent.load(Ordering::Acquire, guard).as_raw()) };
            let res = Cursor::find_harris_michael(&mut cursor, &key, guard);
            if let Ok(found) = res {
                if (found) {
                    return;
                } else {
                    match Cursor::insert(&mut cursor, owned, guard) {
                        Err(n) => owned = n,
                        Ok(()) => {
                            self.buckets
                                .get(child_index, guard)
                                .store(cursor.curr(), Ordering::Release);
                            return;
                        }
                    }
                }
            }
        }
    }

    fn initialize_bucket<'s>(&'s self, bucket_index: usize, guard: &'s Guard) {
        let mut current = self
            .buckets
            .get(bucket_index, guard)
            .load(Ordering::Acquire, guard);
        if (bucket_index == 0 && current.is_null()) {
            if self.list.harris_herlihy_shavit_insert(0, None, guard) {
                let cursor = self.list.head(guard);
                self.buckets.get(0, guard).compare_and_set(
                    current,
                    cursor.curr(),
                    Ordering::AcqRel,
                    guard,
                );
            }
        }

        let parent_index: usize = self.get_parent(bucket_index);
        let mut parent = self
            .buckets
            .get(parent_index, guard)
            .load(Ordering::Acquire, guard);
        if (parent.is_null()) {
            self.initialize_bucket(parent_index, guard);
        }

        self.make_sentinel(parent_index, bucket_index, guard);
    }

    /// Creates a cursor and moves it to the bucket for the given index.  If the bucket doesn't
    /// exist, recursively initializes the buckets.
    fn lookup_bucket<'s>(&'s self, index: usize, guard: &'s Guard) -> Cursor<'s, usize, Option<V>> {
        let pointer = self
            .buckets
            .get(index, guard)
            .load(Ordering::Acquire, guard);
        if (pointer.is_null()) {
            self.initialize_bucket(index, guard);
        }
        let pointer = self.buckets.get(index, guard);
        let mut cursor =
            unsafe { Cursor::from_raw(pointer, pointer.load(Ordering::Acquire, guard).as_raw()) };
        return cursor;
    }

    /// Moves the bucket cursor returned from `lookup_bucket` to the position of the given key.
    /// Returns `(size, found, cursor)`
    fn find<'s>(
        &'s self,
        key: &usize,
        guard: &'s Guard,
    ) -> (usize, bool, Cursor<'s, usize, Option<V>>) {
        let ordinary_key = (*key | SplitOrderedList::<V>::HI_MASK).reverse_bits();

        loop {
            let size: usize = self.size.load(Ordering::Acquire);
            let mut cursor = self.lookup_bucket((*key) % size, guard);
            let res = Cursor::find_harris_michael(&mut cursor, &ordinary_key, guard);
            if let Ok(found) = res {
                return (size, found, cursor);
            }
        }
    }

    fn assert_valid_key(key: usize) {
        assert!(key.leading_zeros() != 0);
    }
}

impl<V> SplitOrderedList<V> {
    pub fn lookup<'a>(&'a self, key: &usize, guard: &'a Guard) -> Option<&'a V> {
        Self::assert_valid_key(*key);

        let (size, found, cursor) = self.find(key, guard);
        if found {
            match cursor.lookup().as_ref().map(|n| (**n).as_ref()) {
                Some(opt) => {
                    return opt;
                }
                None => {
                    return None;
                }
            }
        } else {
            return None;
        }
    }

    pub fn insert(&self, key: &usize, value: V, guard: &Guard) -> Result<(), V> {
        Self::assert_valid_key(*key);

        let ordinary_key = (*key | SplitOrderedList::<V>::HI_MASK).reverse_bits();
        let mut owned = Owned::new(Node::new(ordinary_key, Some(value)));
        loop {
            let (size, found, mut cursor) = self.find(key, guard);
            if (found) {
                let val = owned.into_box().into_value();
                return Err(val.unwrap());
            }
            match cursor.insert(owned, guard) {
                Err(n) => owned = n,
                Ok(()) => {
                    break;
                }
            }
        }

        let count = self.count.fetch_add(1, Ordering::AcqRel);
        let size = self.size.load(Ordering::Acquire);
        if (count / size > SplitOrderedList::<V>::LOAD_FACTOR) {
            self.size.compare_and_swap(size, size * 2, Ordering::AcqRel);
        }
        return Ok(());
    }

    pub fn delete<'a>(&'a self, key: &usize, guard: &'a Guard) -> Result<&'a V, ()> {
        Self::assert_valid_key(*key);

        loop {
            let (size, found, cursor) = self.find(key, guard);
            if !found {
                return Err(());
            }
            match cursor.delete(guard) {
                Err(()) => continue,
                Ok(value) => {
                    self.count.fetch_sub(1, Ordering::AcqRel);
                    return value.as_ref().ok_or(());
                }
            }
        }
    }
}
