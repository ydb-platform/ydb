=============
 zope.schema
=============

.. image:: https://img.shields.io/pypi/v/zope.schema.svg
   :target: https://pypi.org/project/zope.schema/
   :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/zope.schema.svg
   :target: https://pypi.org/project/zope.schema/
   :alt: Supported Python versions

.. image:: https://github.com/zopefoundation/zope.schema/workflows/tests/badge.svg
   :target: https://github.com/zopefoundation/zope.schema/actions?query=workflow%3Atests
   :alt: Tests Status

.. image:: https://readthedocs.org/projects/zopeschema/badge/?version=latest
   :target: https://zopeschema.readthedocs.org/en/latest/
   :alt: Documentation Status

.. image:: https://coveralls.io/repos/github/zopefoundation/zope.schema/badge.svg
   :target: https://coveralls.io/github/zopefoundation/zope.schema
   :alt: Code Coverage

Schemas extend the notion of interfaces to detailed descriptions of
``Attributes`` (but not methods).  Every schema is an interface and
specifies the public fields of an object.  A *field* roughly
corresponds to an attribute of a Python object.  But a Field provides
space for at least a title and a description.  It can also constrain
its value and provide a validation method.  Besides you can optionally
specify characteristics such as its value being read-only or not
required.

See https://zopeschema.readthedocs.io/ for more information.
