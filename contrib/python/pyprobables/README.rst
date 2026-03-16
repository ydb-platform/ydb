PyProbables
===========

.. image:: https://img.shields.io/badge/license-MIT-blue.svg
    :target: https://opensource.org/licenses/MIT/
    :alt: License
.. image:: https://img.shields.io/github/release/barrust/pyprobables.svg
    :target: https://github.com/barrust/pyprobables/releases
    :alt: GitHub release
.. image:: https://github.com/barrust/pyprobables/workflows/Python%20package/badge.svg
    :target: https://github.com/barrust/pyprobables/actions?query=workflow%3A%22Python+package%22
    :alt: Build Status
.. image:: https://codecov.io/gh/barrust/pyprobables/branch/master/graph/badge.svg?token=OdETiNgz9k
    :target: https://codecov.io/gh/barrust/pyprobables
    :alt: Test Coverage
.. image:: https://readthedocs.org/projects/pyprobables/badge/?version=latest
    :target: http://pyprobables.readthedocs.io/en/latest/?badge=latest
    :alt: Documentation Status
.. image:: https://badge.fury.io/py/pyprobables.svg
    :target: https://pypi.org/project/pyprobables/
    :alt: Pypi Release
.. image:: https://pepy.tech/badge/pyprobables
    :target: https://pepy.tech/project/pyprobables
    :alt: Downloads

**pyprobables** is a pure-python library for probabilistic data structures.
The goal is to provide the developer with a pure-python implementation of
common probabilistic data-structures to use in their work.

To achieve better raw performance, it is recommended supplying an alternative
hashing algorithm that has been compiled in C. This could include using the
md5 and sha512 algorithms provided or installing a third party package and
writing your own hashing strategy. Some options include the murmur hash
`mmh3 <https://github.com/hajimes/mmh3>`__ or those from the
`pyhash <https://github.com/flier/pyfasthash>`__ library. Each data object in
**pyprobables** makes it easy to pass in a custom hashing function.

Read more about how to use `Supplying a pre-defined, alternative hashing strategies`_
or `Defining hashing function using the provided decorators`_.

Installation
------------------

Pip Installation:

::

    $ pip install pyprobables

Conda Installation:
::

    $ conda install -c conda-forge pyprobables

To install from source:

To install `pyprobables`, simply clone the `repository on GitHub
<https://github.com/barrust/pyprobables>`__, then run from the folder:

::

    $ python setup.py install

`pyprobables` supports python 3.10 - 3.14+

For *python 2.7* support, install `release 0.3.2 <https://github.com/barrust/pyprobables/releases/tag/v0.3.2>`__

::

    $ pip install pyprobables==0.7.0


API Documentation
---------------------

The documentation of is hosted on
`readthedocs.io <http://pyprobables.readthedocs.io/en/latest/code.html#api>`__

You can build the documentation locally by running:

::

    $ pip install sphinx
    $ cd docs/
    $ make html



Automated Tests
------------------

To run automated tests, one must simply run the following command from the
downloaded folder:

::

  $ python setup.py test



Quickstart
------------------

Import pyprobables and setup a Bloom Filter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from probables import BloomFilter
    blm = BloomFilter(est_elements=1000, false_positive_rate=0.05)
    blm.add('google.com')
    blm.check('facebook.com')  # should return False
    blm.check('google.com')  # should return True


Import pyprobables and setup a Count-Min Sketch
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from probables import CountMinSketch
    cms = CountMinSketch(width=1000, depth=5)
    cms.add('google.com')  # should return 1
    cms.add('facebook.com', 25)  # insert 25 at once; should return 25


Import pyprobables and setup a Cuckoo Filter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from probables import CuckooFilter
    cko = CuckooFilter(capacity=100, max_swaps=10)
    cko.add('google.com')
    cko.check('facebook.com')  # should return False
    cko.check('google.com')  # should return True


Import pyprobables and setup a Quotient Filter
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from probables import QuotientFilter
    qf = QuotientFilter(quotient=24)
    qf.add('google.com')
    qf.check('facebook.com')  # should return False
    qf.check('google.com')  # should return True


Supplying a pre-defined, alternative hashing strategies
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    from probables import BloomFilter
    from probables.hashes import default_sha256
    blm = BloomFilter(est_elements=1000, false_positive_rate=0.05,
                      hash_function=default_sha256)
    blm.add('google.com')
    blm.check('facebook.com')  # should return False
    blm.check('google.com')  # should return True


.. _use-custom-hashing-strategies:

Defining hashing function using the provided decorators
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code:: python

    import mmh3  # murmur hash 3 implementation (pip install mmh3)
    from probables.hashes import hash_with_depth_bytes
    from probables import BloomFilter

    @hash_with_depth_bytes
    def my_hash(key, depth):
        return mmh3.hash_bytes(key, seed=depth)

    blm = BloomFilter(est_elements=1000, false_positive_rate=0.05, hash_function=my_hash)

.. code:: python

    import hashlib
    from probables.hashes import hash_with_depth_int
    from probables.constants import UINT64_T_MAX
    from probables import BloomFilter

    @hash_with_depth_int
    def my_hash(key, seed=0, encoding="utf-8"):
        max64mod = UINT64_T_MAX + 1
        val = int(hashlib.sha512(key.encode(encoding)).hexdigest(), 16)
        val += seed  # not a good example, but uses the seed value
        return val % max64mod

    blm = BloomFilter(est_elements=1000, false_positive_rate=0.05, hash_function=my_hash)


See the `API documentation <http://pyprobables.readthedocs.io/en/latest/code.html#api>`__
for other data structures available and the
`quickstart page <http://pyprobables.readthedocs.io/en/latest/quickstart.html#quickstart>`__
for more examples!


Changelog
------------------

Please see the `changelog
<https://github.com/barrust/pyprobables/blob/master/CHANGELOG.md>`__ for a list
of all changes.


Backward Compatible Changes
---------------------------

If you are using previously exported probablistic data structures (v0.4.1 or below)
and used the default hashing strategy, you will want to use the following code
to mimic the original default hashing algorithm.

.. code:: python

    from probables import BloomFilter
    from probables.hashes import hash_with_depth_int

    @hash_with_depth_int
    def old_fnv1a(key, depth=1):
        return tmp_fnv_1a(key)

    def tmp_fnv_1a(key):
        max64mod = UINT64_T_MAX + 1
        hval = 14695981039346656073
        fnv_64_prime = 1099511628211
        tmp = map(ord, key)
        for t_str in tmp:
            hval ^= t_str
            hval *= fnv_64_prime
            hval %= max64mod
        return hval

    blm = BloomFilter(filpath="old-file-path.blm", hash_function=old_fnv1a)
