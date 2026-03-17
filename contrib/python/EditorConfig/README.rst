========================
EditorConfig Python Core
========================
.. image:: https://img.shields.io/pypi/v/EditorConfig.svg
    :target: https://pypi.python.org/pypi/EditorConfig

.. image:: https://img.shields.io/pypi/wheel/EditorConfig.svg
    :target: https://pypi.python.org/pypi/EditorConfig

.. image:: https://img.shields.io/pypi/pyversions/EditorConfig.svg
    :target: https://pypi.python.org/pypi/EditorConfig

.. image:: https://secure.travis-ci.org/editorconfig/editorconfig-core-py.svg?branch=master
   :target: http://travis-ci.org/editorconfig/editorconfig-core-py

EditorConfig Python Core provides the same functionality as the
`EditorConfig C Core <https://github.com/editorconfig/editorconfig-core>`_.
EditorConfig Python core can be used as a command line program or as an
importable library.

EditorConfig Project
====================

EditorConfig makes it easy to maintain the correct coding style when switching
between different text editors and between different projects.  The
EditorConfig project maintains a file format and plugins for various text
editors which allow this file format to be read and used by those editors.  For
information on the file format and supported text editors, see the
`EditorConfig website <https://editorconfig.org>`_.

Installation
============

With setuptools::

    sudo python setup.py install

Getting Help
============
For help with the EditorConfig core code, please write to our `mailing list
<http://groups.google.com/group/editorconfig>`_.  Bugs and feature requests
should be submitted to our `issue tracker
<https://github.com/editorconfig/editorconfig/issues>`_.

If you are writing a plugin a language that can import Python libraries, you
may want to import and use the EditorConfig Python Core directly.

Using as a Library
==================

Basic example use of EditorConfig Python Core as a library:

.. code-block:: python

    from editorconfig import get_properties, EditorConfigError

    filename = "/home/zoidberg/humans/anatomy.md"

    try:
        options = get_properties(filename)
    except EditorConfigError:
        print("Error occurred while getting EditorConfig properties")
    else:
        for key, value in options.items():
            print(f"{key}={value}")

For details, please take a look at the `online documentation
<http://pydocs.editorconfig.org>`_.

Running Test Cases
==================

`Cmake <http://www.cmake.org>`_ has to be installed first. Run the test cases
using the following commands::

    export PYTHONPATH=$(pwd)
    cmake .
    ctest .

Use ``-DPYTHON_EXECUTABLE`` to run the tests using an alternative versions of
Python (e.g. Python 3.12)::

    cmake -DPYTHON_EXECUTABLE=/usr/bin/python3.12 .
    ctest .

License
=======

See COPYING file for licensing details.
