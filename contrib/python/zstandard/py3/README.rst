================
python-zstandard
================

| |ci-test| |ci-wheel| |ci-typing| |ci-sdist| |ci-anaconda| |ci-sphinx|

This project provides Python bindings for interfacing with the
`Zstandard <http://www.zstd.net>`_ compression library. A C extension
and CFFI interface are provided.

The primary goal of the project is to provide a rich interface to the
underlying C API through a Pythonic interface while not sacrificing
performance. This means exposing most of the features and flexibility
of the C API while not sacrificing usability or safety that Python provides.

The canonical home for this project is
https://github.com/indygreg/python-zstandard.

For usage documentation, see https://python-zstandard.readthedocs.org/.

.. |ci-test| image:: https://github.com/indygreg/python-zstandard/workflows/.github/workflows/test.yml/badge.svg
     :target: https://github.com/indygreg/python-zstandard/blob/main/.github/workflows/test.yml

.. |ci-wheel| image:: https://github.com/indygreg/python-zstandard/workflows/.github/workflows/wheel.yml/badge.svg
     :target: https://github.com/indygreg/python-zstandard/blob/main/.github/workflows/wheel.yml

.. |ci-typing| image:: https://github.com/indygreg/python-zstandard/workflows/.github/workflows/typing.yml/badge.svg
     :target: https://github.com/indygreg/python-zstandard/blob/main/.github/workflows/typing.yml

.. |ci-sdist| image:: https://github.com/indygreg/python-zstandard/workflows/.github/workflows/sdist.yml/badge.svg
     :target: https://github.com/indygreg/python-zstandard/blob/main/.github/workflows/sdist.yml

.. |ci-anaconda| image:: https://github.com/indygreg/python-zstandard/workflows/.github/workflows/anaconda.yml/badge.svg
     :target: https://github.com/indygreg/python-zstandard/blob/main/.github/workflows/anaconda.yml

.. |ci-sphinx| image:: https://github.com/indygreg/python-zstandard/workflows/.github/workflows/sphinx.yml/badge.svg
     :target: https://github.com/indygreg/python-zstandard/blob/main/.github/workflows/sphinx.yml
