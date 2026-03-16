## Walrus

![](http://media.charlesleifer.com/blog/photos/walrus-logo-0.png)

Lightweight Python utilities for working with [Redis](http://redis.io).

The purpose of [walrus](https://github.com/coleifer/walrus) is to make working
with Redis in Python a little easier. Rather than ask you to learn a new
library, walrus subclasses and extends the popular `redis-py` client, allowing
it to be used as a drop-in replacement. In addition to all the features in
`redis-py`, walrus adds support for some newer commands, including full support
for streams and consumer groups.

walrus consists of:

* Pythonic container classes for the Redis data-types:
    * [Hash](https://walrus.readthedocs.io/en/latest/containers.html#hashes)
    * [List](https://walrus.readthedocs.io/en/latest/containers.html#lists)
    * [Set](https://walrus.readthedocs.io/en/latest/containers.html#sets)
    * [Sorted Set](https://walrus.readthedocs.io/en/latest/containers.html#sorted-sets-zset)
    * [HyperLogLog](https://walrus.readthedocs.io/en/latest/containers.html#hyperloglog)
    * [Array](https://walrus.readthedocs.io/en/latest/containers.html#arrays) (custom type)
    * [BitField](https://walrus.readthedocs.io/en/latest/containers.html#bitfield)
    * [BloomFilter](https://walrus.readthedocs.io/en/latest/containers.html#bloomfilter)
    * [**Streams**](https://walrus.readthedocs.io/en/latest/streams.html)
* [Autocomplete](https://walrus.readthedocs.io/en/latest/autocomplete.html)
* [Cache](https://walrus.readthedocs.io/en/latest/cache.html) implementation that exposes several decorators for caching function and method calls.
* [Full-text search](https://walrus.readthedocs.io/en/latest/full-text-search.html) supporting set operations.
* [Graph store](https://walrus.readthedocs.io/en/latest/graph.html)
* [Rate-limiting](https://walrus.readthedocs.io/en/latest/rate-limit.html)
* [Locking](https://walrus.readthedocs.io/en/latest/api.html#walrus.Lock)
* **Experimental** active-record style [Models](https://walrus.readthedocs.io/en/latest/models.html) that support persisting structured information and performing complex queries using secondary indexes.
* More? [More!](https://walrus.readthedocs.io)

### Models

Persistent structures implemented on top of Hashes. Supports secondary indexes to allow filtering on equality, inequality, ranges, less/greater-than, and a basic full-text search index. The full-text search features a boolean search query parser, porter stemmer, stop-word filtering, and optional double-metaphone implementation.

### Found a bug?

![](http://media.charlesleifer.com/blog/photos/p1420743625.21.png)

Please open a [github issue](https://github.com/coleifer/walrus/issues/new) and I will try my best to fix it!
