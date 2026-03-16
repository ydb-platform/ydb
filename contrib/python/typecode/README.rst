========
TypeCode
========

- license: Apache-2.0
- copyright: copyright (c) nexB. Inc. and others
- homepage_url: https://github.com/nexB/typecode
- keywords: filetype, mimetype, libmagic, scancode-toolkit, typecode

TypeCode provides comprehensive filetype and mimetype detection using multiple
detectors including libmagic (included as a dependency for Linux, Windows and
macOS) and Pygments. It started as library in scancode-toolkit.
Visit https://aboutcode.org and https://github.com/nexB/ for support and download.


We run CI tests on:

 - Travis https://travis-ci.org/github/nexB/typecode
 - Azure pipelines https://dev.azure.com/nexB/typecode/_build

To install this package with its full capability (where the binaries for
libmagic are installed), use the `full` option::

    pip install typecode[full]

If you want to use the version of libmagic (possibly) provided by your operating
system, use the `minimal` option::

    pip install typecode

In this case, you will need to provide a working libmagic and its database
available in one of these ways:

- **a typecode-libmagic plugin**: See the standard ones at
  https://github.com/nexB/scancode-plugins/tree/main/builtins
  These can either bundle a libmagic library and its magic database or expose a
  system-installed libmagic.
  They do so by providing a plugin entry point as a ``scancode_location_provider``
  for ``typecode_libmagic`` which points to a callable that must return a mapping
  with these two keys:

    - 'typecode.libmagic.dll': the absolute path to a libmagic DLL
    - 'typecode.libmagic.db': the absolute path to a libmagic 'magic.mgc' database

  See for example:

    - https://github.com/nexB/scancode-plugins/blob/4da5fe8a5ab1c87b9b4af9e54d7ad60e289747f5/builtins/typecode_libmagic-linux/setup.py#L42
    - https://github.com/nexB/scancode-plugins/blob/4da5fe8a5ab1c87b9b4af9e54d7ad60e289747f5/builtins/typecode_libmagic-linux/src/typecode_libmagic/__init__.py#L32

- **environment variables**:

  - TYPECODE_LIBMAGIC_PATH: the absolute path to a libmagic DLL
  - TYPECODE_LIBMAGIC_DB_PATH: the absolute path to a libmagic 'magic.mgc' database

- **a system-installed libmagic and its database availale in the system PATH**:


The supported libmagic version is 5.39.


To set up the development environment::

    source configure --dev

To run unit tests::

    pytest -vvs -n 2

To clean up development environment::

    ./configure --clean


To update Pygment to a newer vendored version use vendy:

  - Update the version of pygments in ``pyproject.toml``
  - Run ``vendy``
  - Update the src/typecpde/pygments_lexers_mapping.py
    and src/typecode/pygments_lexers.py scripts accordingly, including their
    ABOUT files

