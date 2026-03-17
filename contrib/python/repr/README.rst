========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |requires|
        |
    * - package
      - |version| |downloads| |wheel| |supported-versions| |supported-implementations|

.. |docs| image:: https://readthedocs.org/projects/python-repr/badge/?style=flat
    :target: https://readthedocs.org/projects/python-repr
    :alt: Documentation Status

.. |travis| image:: https://travis-ci.org/svetlyak40wt/python-repr.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.org/svetlyak40wt/python-repr

.. |requires| image:: https://requires.io/github/svetlyak40wt/python-repr/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/svetlyak40wt/python-repr/requirements/?branch=master

.. |version| image:: https://img.shields.io/pypi/v/repr.svg?style=flat
    :alt: PyPI Package latest release
    :target: https://pypi.python.org/pypi/repr

.. |downloads| image:: https://img.shields.io/pypi/dm/repr.svg?style=flat
    :alt: PyPI Package monthly downloads
    :target: https://pypi.python.org/pypi/repr

.. |wheel| image:: https://img.shields.io/pypi/wheel/repr.svg?style=flat
    :alt: PyPI Wheel
    :target: https://pypi.python.org/pypi/repr

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/repr.svg?style=flat
    :alt: Supported versions
    :target: https://pypi.python.org/pypi/repr

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/repr.svg?style=flat
    :alt: Supported implementations
    :target: https://pypi.python.org/pypi/repr


.. end-badges

A magic shortcut to generate __repr__ methods for your classes.

* Free software: BSD license

Installation
============

::

    pip install repr

This package contains a single module ``magic_repr`` called so
to not conflict with standart python's ``repr``.

Reasoning
=========

What do you think each time, writing such code?

.. code:: python

  def __repr__(self):
      return """
  Issue(changelog={self.changelog},
        type={self.type},
        comment={self.comment},
        created_at={self.created_at},
        resolved_at={self.resolved_at})""".format(self=self).strip().encode('utf-8')

Isn't this much better and readable?

.. code:: python

      __repr__ = make_repr('changelog', 'type', 'comment', 'created_at', 'resolved_at')


And this produces much nicer output:

.. code:: python

  <Issue changelog=<Changelog namespace=u'python'
                              name=u'geocoder'
                              source=u'https://github.com/DenisCarriere/geocoder'>
         type=u'wrong-version-content'
         comment=u'AllMyChanges should take release notes from the web site.'
         created_at=datetime.datetime(2016, 6, 17, 6, 44, 52, 16760, tzinfo=<UTC>)
         resolved_at=None>

Another advantage of the magic_repr
-----------------------------------

Is it's recursiveness. If you use ``magic_repr`` for your objects and they
include each other, then representation of the parent object will include
child objects properly nested:

.. code:: python

  <Foo bars={1: <Bar first=1
                     second=2
                     third=3>,
             2: <Bar first=1
                     second=2
                     third=3>,
             u'три': <Bar first=1
                          second=2
                          third=3>}>

And all this for free! Just do ``__repr__ = make_repr()``.

Usage
=====

For simple cases it is enough to call ``make_repr`` without arguments. It will figure out
which attributes object has and will output them sorted alphabetically.

You can also specify which attributes you want to include in "representaion":

.. code:: python

  from magic_repr import make_repr

  __repr__ = make_repr('foo', 'bar')

And to specify a function to create a value for an attribute, using keywords:

.. code:: python

  from magic_repr import make_repr
  
  class Some(object):
      def is_active(self):
          return True

  Some.__repr__ = make_repr(active=Some.is_active)

Pay attention, that in this case ``__repr__`` was created after the class definition.
This is because inside of the class it can't reference itself.

Documentation
=============

https://python-repr.readthedocs.io/

Development
===========

To run the all tests run::

    tox

Note, to combine the coverage data from all the tox environments run:

.. list-table::
    :widths: 10 90
    :stub-columns: 1

    - - Windows
      - ::

            set PYTEST_ADDOPTS=--cov-append
            tox

    - - Other
      - ::

            PYTEST_ADDOPTS=--cov-append tox
