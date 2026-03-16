Flake8 Single Line Import Plugin
================================

A Flake8 plugin that requires single line imports.

|Status| |PackageVersion| |PythonVersions|

Introduction
------------

The following will result in an error::

    from foo import bar, baz

It should be rewritten as::

    from foo import bar
    from foo import baz

Installation
------------

::

    $ pip install flake8-import-single
    $ pip install --upgrade flake8-import-single

Plugin for Flake8
-----------------

::

    $ flake8 --version
    3.5.0 (flake8-import-single: 0.1.2, mccabe: 0.6.1, pycodestyle: 2.3.1, pyflakes: 1.6.0)

Thanks
------

Much thanks goes out to flake8-print_ as the basis for
this plugin.

.. _flake8-print: https://github.com/JBKahn/flake8-print

.. |PackageVersion| image:: https://img.shields.io/pypi/v/flake8-import-single.svg?style=flat
    :alt: PyPI version
    :target: https://pypi.org/project/flake8-import-single

.. |PythonVersions| image:: https://img.shields.io/pypi/pyversions/flake8-import-single.svg
    :alt: Supported Python versions
    :target: https://pypi.org/project/flake8-import-single

.. |Status| image:: https://img.shields.io/circleci/project/github/awiddersheim/flake8-import-single/master.svg
    :alt: Build
    :target: https://circleci.com/gh/awiddersheim/flake8-import-single
