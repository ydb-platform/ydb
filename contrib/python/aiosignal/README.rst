=========
aiosignal
=========

.. image:: https://github.com/aio-libs/aiosignal/workflows/CI/badge.svg
   :target: https://github.com/aio-libs/aiosignal/actions?query=workflow%3ACI
   :alt: GitHub status for master branch

.. image:: https://codecov.io/gh/aio-libs/aiosignal/branch/master/graph/badge.svg
   :target: https://codecov.io/gh/aio-libs/aiosignal
   :alt: codecov.io status for master branch

.. image:: https://badge.fury.io/py/aiosignal.svg
   :target: https://pypi.org/project/aiosignal
   :alt: Latest PyPI package version

.. image:: https://readthedocs.org/projects/aiosignal/badge/?version=latest
   :target: https://aiosignal.readthedocs.io/
   :alt: Latest Read The Docs

.. image:: https://img.shields.io/discourse/topics?server=https%3A%2F%2Faio-libs.discourse.group%2F
   :target: https://aio-libs.discourse.group/
   :alt: Discourse group for io-libs

.. image:: https://badges.gitter.im/Join%20Chat.svg
   :target: https://gitter.im/aio-libs/Lobby
   :alt: Chat on Gitter

Introduction
============

A project to manage callbacks in `asyncio` projects.

``Signal`` is a list of registered asynchronous callbacks.

The signal's life-cycle has two stages: after creation its content
could be filled by using standard list operations: ``sig.append()``
etc.

After you call ``sig.freeze()`` the signal is *frozen*: adding, removing
and dropping callbacks is forbidden.

The only available operation is calling the previously registered
callbacks by using ``await sig.send(data)``.

For concrete usage examples see the `Signals
<https://docs.aiohttp.org/en/stable/web_advanced.html#aiohttp-web-signals>
section of the `Web Server Advanced
<https://docs.aiohttp.org/en/stable/web_advanced.html>` chapter of the `aiohttp
documentation`_.


Installation
------------

::

   $ pip install aiosignal

The library requires Python 3.6 or newer.


Documentation
=============

https://aiosignal.readthedocs.io/

Communication channels
======================

*gitter chat* https://gitter.im/aio-libs/Lobby

Requirements
============

- Python >= 3.6
- frozenlist >= 1.0.0

License
=======

``aiosignal`` is offered under the Apache 2 license.

Source code
===========

The project is hosted on GitHub_

Please file an issue in the `bug tracker
<https://github.com/aio-libs/aiosignal/issues>`_ if you have found a bug
or have some suggestions to improve the library.

.. _GitHub: https://github.com/aio-libs/aiosignal
.. _aiohttp documentation: https://docs.aiohttp.org/
