pygtrie
=======

.. image:: https://readthedocs.org/projects/pygtrie/badge/?version=latest
   :target: http://pygtrie.readthedocs.io/en/latest/
   :alt: Documentation build status (latest)

.. image:: https://readthedocs.org/projects/pygtrie/badge/?version=stable
   :target: http://pygtrie.readthedocs.io/en/stable/
   :alt: Documentation build status (stable)

.. image:: https://api.travis-ci.com/mina86/pygtrie.svg
   :target: https://travis-ci.com/mina86/pygtrie
   :alt: Continuous integration status

pygtrie is a pure Python implementation of a trie data structure
compatible with Python 2.x and Python 3.x.

`Trie data structure <http://en.wikipedia.org/wiki/Trie>`_, also known
as radix or prefix tree, is a tree associating keys to values where
all the descendants of a node have a common prefix (associated with
that node).

The trie module contains ``Trie``, ``CharTrie`` and ``StringTrie``
classes each implementing a mutable mapping interface, i.e. ``dict``
interface.  As such, in most circumstances, ``Trie`` could be used as
a drop-in replacement for a ``dict``, but the prefix nature of the
data structure is trie’s real strength.

The module also contains ``PrefixSet`` class which uses a trie to
store a set of prefixes such that a key is contained in the set if it
or its prefix is stored in the set.

Features
--------

- A full mutable mapping implementation.

- Supports iterating over as well as deleting a subtrie.

- Supports prefix checking as well as shortest and longest prefix
  look-up.

- Extensible for any kind of user-defined keys.

- A PrefixSet supports “all keys starting with given prefix” logic.

- Can store any value including None.

Installation
------------

To install pygtrie, simply run::

    pip install pygtrie

or by adding line such as::

    pygtrie == 2.*

to project’s `requirements file
<https://pip.pypa.io/en/latest/user_guide/#requirements-files>`_.
Alternatively, if installation from source is desired, it can be
achieved by executing::

    python setup.py install
