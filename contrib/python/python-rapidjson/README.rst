.. -*- coding: utf-8 -*-
.. :Project:   python-rapidjson -- Introduction
.. :Author:    Ken Robbins <ken@kenrobbins.com>
.. :License:   MIT License
.. :Copyright: © 2015 Ken Robbins
.. :Copyright: © 2016, 2017, 2018, 2020, 2022, 2024, 2025 Lele Gaifax
..

==================
 python-rapidjson
==================

Python wrapper around RapidJSON
===============================

:Authors: Ken Robbins <ken@kenrobbins.com>; Lele Gaifax <lele@metapensiero.it>
:License: `MIT License`__
:Status: |build| |doc|

__ https://raw.githubusercontent.com/python-rapidjson/python-rapidjson/master/LICENSE
.. |build| image:: https://travis-ci.org/python-rapidjson/python-rapidjson.svg?branch=master
   :target: https://travis-ci.org/python-rapidjson/python-rapidjson
   :alt: Build status
.. |doc| image:: https://readthedocs.org/projects/python-rapidjson/badge/?version=latest
   :target: https://readthedocs.org/projects/python-rapidjson/builds/
   :alt: Documentation status

RapidJSON_ is an extremely fast C++ JSON parser and serialization library: this module
wraps it into a Python 3 extension, exposing its serialization/deserialization (to/from
either ``bytes``, ``str`` or *file-like* instances) and `JSON Schema`__ validation
capabilities.

Latest version documentation is automatically rendered by `Read the Docs`__.

__ http://json-schema.org/documentation.html
__ https://python-rapidjson.readthedocs.io/en/latest/


Getting Started
---------------

First install ``python-rapidjson``:

.. code-block:: bash

    $ pip install python-rapidjson

or, if you prefer `Conda`__:

.. code-block:: bash

    $ conda install -c conda-forge python-rapidjson

__ https://conda.io/docs/

Basic usage looks like this:

.. code-block:: python

    >>> import rapidjson
    >>> data = {'foo': 100, 'bar': 'baz'}
    >>> rapidjson.dumps(data)
    '{"foo":100,"bar":"baz"}'
    >>> rapidjson.loads('{"bar":"baz","foo":100}')
    {'bar': 'baz', 'foo': 100}
    >>>
    >>> class Stream:
    ...   def write(self, data):
    ...      print("Chunk:", data)
    ...
    >>> rapidjson.dump(data, Stream(), chunk_size=5)
    Chunk: b'{"foo'
    Chunk: b'":100'
    Chunk: b',"bar'
    Chunk: b'":"ba'
    Chunk: b'z"}'

Most functionalities are exposed both as *functions* and as *classes*.

The following uses a *relaxed syntax* ``Decoder`` instance, that handles JSONC__ and
*trailing commas*:

__ https://jsonc.org/

.. code-block:: python

    >>> from rapidjson import Decoder
    >>> from rapidjson import PM_COMMENTS, PM_TRAILING_COMMAS
    >>> decoder = Decoder(parse_mode=PM_COMMENTS | PM_TRAILING_COMMAS)
    >>> decoder('''
    ... {
    ...     "bar": /* Block comment */ "baz",
    ...     "foo":100, // Trailing comma and comment
    ... }
    ... ''')
    {'bar': 'baz', 'foo': 100}


Development
-----------

If you want to install the development version (maybe to contribute fixes or
enhancements) you may clone the repository:

.. code-block:: bash

    $ git clone --recursive https://github.com/python-rapidjson/python-rapidjson.git

.. note:: The ``--recursive`` option is needed because we use a *submodule* to
          include RapidJSON_ sources. Alternatively you can do a plain
          ``clone`` immediately followed by a ``git submodule update --init``.

          Alternatively, if you already have (a *compatible* version of)
          RapidJSON includes around, you can compile the module specifying
          their location with the option ``--rj-include-dir``, for example:

          .. code-block:: shell

             $ python3 setup.py build --rj-include-dir=/usr/include/rapidjson

A set of makefiles implement most common operations, such as *build*, *check*
and *release*; see ``make help`` output for a list of available targets.


Performance
-----------

``python-rapidjson`` tries to be as performant as possible while staying
compatible with the ``json`` module.

See `this section`__ in the documentation for a comparison with other JSON libraries.

__ https://python-rapidjson.readthedocs.io/en/latest/benchmarks.html


Incompatibility
---------------

Although we tried to implement an API similar to the standard library ``json``, being a
strict *drop-in* replacement in not our goal and we have decided to depart from there in
some aspects. See `this section`__ in the documentation for further details.

__ https://python-rapidjson.readthedocs.io/en/latest/quickstart.html#incompatibilities

.. _RapidJSON: http://rapidjson.org/
