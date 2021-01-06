# Split-ordered non-blocking concurrent hash table

The implementation is based on

> Ori Shalev and Nir Shavit. 2006. Split-ordered lists: Lock-free extensible hash tables. J. ACM 53, 3 (May 2006), 379–405. DOI: [link](https://dl.acm.org/doi/abs/10.1145/1147954.1147958)

with an additional improvement of bucket reference storage by adding growable arrays.