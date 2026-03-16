===========
Ordered Set
===========

.. image:: https://badge.fury.io/py/orderedset.png
    :target: http://badge.fury.io/py/orderedset

.. image:: https://travis-ci.org/simonpercivall/orderedset.png?branch=master
    :target: https://travis-ci.org/simonpercivall/orderedset

.. image:: https://pypip.in/d/orderedset/badge.png
    :target: https://crate.io/packages/orderedset?version=latest


An Ordered Set implementation in Cython. Based on `Raymond Hettinger's OrderedSet recipe`_.

Example:

.. code-block:: python

    >>> from orderedset import OrderedSet
    >>> oset = OrderedSet([1, 2, 3])
    >>> oset
    OrderedSet([1, 2, 3])
    >>> oset | [5, 4, 3, 2, 1]
    OrderedSet([1, 2, 3, 5, 4])

* Free software: BSD license
* Documentation: http://orderedset.rtfd.org.

Features
--------

* Works like a regular set, but remembers insertion order;
* Is approximately 5 times faster than the pure Python implementation overall
  (and 5 times slower than `set`);
* Compatible with Python 2.7 through 3.8;
* Supports the full set interface;
* Supports some list methods, like `index` and `__getitem__`.
* Supports set methods against iterables.

.. _`Raymond Hettinger's OrderedSet recipe`: http://code.activestate.com/recipes/576694/
