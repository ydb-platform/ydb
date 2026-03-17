.. image:: https://github.com/zopefoundation/RestrictedPython/actions/workflows/tests.yml/badge.svg
    :target: https://github.com/zopefoundation/RestrictedPython/actions/workflows/tests.yml

.. image:: https://coveralls.io/repos/github/zopefoundation/RestrictedPython/badge.svg?branch=master
    :target: https://coveralls.io/github/zopefoundation/RestrictedPython?branch=master

.. image:: https://readthedocs.org/projects/restrictedpython/badge/
    :target: https://restrictedpython.readthedocs.org/
    :alt: Documentation Status

.. image:: https://img.shields.io/pypi/v/RestrictedPython.svg
    :target: https://pypi.org/project/RestrictedPython/
    :alt: Current version on PyPI

.. image:: https://img.shields.io/pypi/pyversions/RestrictedPython.svg
    :target: https://pypi.org/project/RestrictedPython/
    :alt: Supported Python versions

.. image:: https://github.com/zopefoundation/RestrictedPython/raw/master/docs/logo.jpg

================
RestrictedPython
================

RestrictedPython is a tool that helps to define a subset of the Python language which allows to provide a program input into a trusted environment.
RestrictedPython is not a sandbox system or a secured environment, but it helps to define a trusted environment and execute untrusted code inside of it.

.. warning::

   RestrictedPython only supports CPython. It does _not_ support PyPy and other Python implementations as it cannot provide its restrictions there.

For full documentation please see http://restrictedpython.readthedocs.io/.

Example
=======

To give a basic understanding what RestrictedPython does here two examples:

An unproblematic code example
-----------------------------

Python allows you to execute a large set of commands.
This would not harm any system.

.. code-block:: pycon

    >>> from RestrictedPython import compile_restricted
    >>> from RestrictedPython import safe_globals
    >>>
    >>> source_code = """
    ... def example():
    ...     return 'Hello World!'
    ... """
    >>>
    >>> loc = {}
    >>> byte_code = compile_restricted(source_code, '<inline>', 'exec')
    >>> exec(byte_code, safe_globals, loc)
    >>>
    >>> loc['example']()
    'Hello World!'

Problematic code example
------------------------

This example directly executed in Python could harm your system.

.. code-block:: pycon

    >>> from RestrictedPython import compile_restricted
    >>> from RestrictedPython import safe_globals
    >>>
    >>> source_code = """
    ... import os
    ...
    ... os.listdir('/')
    ... """
    >>> byte_code = compile_restricted(source_code, '<inline>', 'exec')
    >>> exec(byte_code, safe_globals, {})
    Traceback (most recent call last):
    ImportError: __import__ not found

Contributing to RestrictedPython
--------------------------------

If you want to help maintain RestrictedPython and contribute, please refer to
the documentation `Contributing page
<https://restrictedpython.readthedocs.io/en/latest/contributing/index.html>`_.
