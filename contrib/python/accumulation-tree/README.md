`accumulation_tree`
===================

[![CircleCI][circleci-img]][circleci-url]

[circleci-url]: https://circleci.com/gh/tkluck/accumulation_tree/
[circleci-img]: https://img.shields.io/circleci/project/github/tkluck/pac4cli.svg

A red/black tree which also stores partial aggregations at each node, making
getting aggregations of key range slices an O(log(N)) operation.

This data structure is very similar to a [Fenwick tree][wiki] and can be used
for the same purpose. The main difference with the description on Wikipedia is
that we allocate nodes on the heap instead of using an implicit tree structure
on a flat array.

This implementation was written specifically for use in [tdigest][tdigest-github],
and borrows code from [bintrees][bintrees-github].

[tdigest-github]: https://github.com/CamDavidsonPilon/tdigest/
[bintrees-github]: https://github.com/mozman/bintrees/
[wiki]: https://en.wikipedia.org/wiki/Fenwick_tree

Synopsis
--------
```python
>>> from accumulation_tree import AccumulationTree
>>> t = AccumulationTree(lambda x: x)
>>> N = 10000
>>> for x in range(N):
...    t.insert(x,x)
>>> t.get_accumulation(0, 2)
1
>>> t.get_accumulation(0, 5)
10
>>> all(t.get_accumulation(0, x) == x*(x-1)/2 for x in range(N))
True

```
