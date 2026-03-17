===================
aiohttp-middlewares
===================

.. image:: https://github.com/playpauseandstop/aiohttp-middlewares/actions/workflows/ci.yml/badge.svg
    :target: https://github.com/playpauseandstop/aiohttp-middlewares/actions/workflows/ci.yml
    :alt: CI Workflow

.. image:: https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white
    :target: https://github.com/pre-commit/pre-commit
    :alt: pre-commit

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
    :target: https://github.com/psf/black
    :alt: black

.. image:: https://img.shields.io/pypi/v/aiohttp-middlewares.svg
    :target: https://pypi.org/project/aiohttp-middlewares/
    :alt: Latest Version

.. image:: https://img.shields.io/pypi/pyversions/aiohttp-middlewares.svg
    :target: https://pypi.org/project/aiohttp-middlewares/
    :alt: Python versions

.. image:: https://img.shields.io/pypi/l/aiohttp-middlewares.svg
    :target: https://github.com/playpauseandstop/aiohttp-middlewares/blob/main/LICENSE
    :alt: BSD License

.. image:: https://coveralls.io/repos/playpauseandstop/aiohttp-middlewares/badge.svg?branch=main&service=github
    :target: https://coveralls.io/github/playpauseandstop/aiohttp-middlewares
    :alt: Coverage

.. image:: https://readthedocs.org/projects/aiohttp-middlewares/badge/?version=latest
    :target: http://aiohttp-middlewares.readthedocs.org/en/latest/
    :alt: Documentation

Collection of useful middlewares for `aiohttp.web`_ applications.

- Works on `Python`_ 3.8+
- Works with `aiohttp.web`_ 3.8.1+
- BSD licensed
- Latest documentation `on Read The Docs
  <https://aiohttp-middlewares.readthedocs.io/>`_
- Source, issues, and pull requests `on GitHub
  <https://github.com/playpauseandstop/aiohttp-middlewares>`_

.. _`aiohttp.web`: https://docs.aiohttp.org/en/stable/web.html
.. _`Python`: https://www.python.org/

Quick Start
===========

By default ``aiohttp.web`` does not provide `many built-in middlewares
<https://docs.aiohttp.org/en/stable/web_reference.html#middlewares>`_ for
standart web-development needs such as: handling errors, shielding view
handlers, or providing CORS headers.

``aiohttp-middlewares`` tries to fix this by providing several middlewares that
aims to cover most common web-development needs.

For example, to enable CORS headers for ``http://localhost:8081`` origin and
handle errors for ``aiohttp.web`` application you need to,

.. code-block:: python

    from aiohttp import web
    from aiohttp_middlewares import (
        cors_middleware,
        error_middleware,
    )


    app = web.Application(
        middlewares=(
            cors_middleware(origins=("http://localhost:8081",)),
            error_middleware(),
        )
    )

Check `documentation <https://aiohttp-middlewares.readthedocs.io/>`_ for
all available middlewares and available initialization options.
