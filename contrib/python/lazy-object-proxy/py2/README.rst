========
Overview
========

.. start-badges

.. list-table::
    :stub-columns: 1

    * - docs
      - |docs|
    * - tests
      - | |travis| |appveyor| |requires|
        | |coveralls| |codecov|
    * - package
      - | |version| |wheel| |supported-versions| |supported-implementations|
        | |commits-since|
.. |docs| image:: https://readthedocs.org/projects/python-lazy-object-proxy/badge/?style=flat
    :target: https://readthedocs.org/projects/python-lazy-object-proxy
    :alt: Documentation Status

.. |travis| image:: https://api.travis-ci.com/ionelmc/python-lazy-object-proxy.svg?branch=master
    :alt: Travis-CI Build Status
    :target: https://travis-ci.com/github/ionelmc/python-lazy-object-proxy

.. |appveyor| image:: https://ci.appveyor.com/api/projects/status/github/ionelmc/python-lazy-object-proxy?branch=master&svg=true
    :alt: AppVeyor Build Status
    :target: https://ci.appveyor.com/project/ionelmc/python-lazy-object-proxy

.. |requires| image:: https://requires.io/github/ionelmc/python-lazy-object-proxy/requirements.svg?branch=master
    :alt: Requirements Status
    :target: https://requires.io/github/ionelmc/python-lazy-object-proxy/requirements/?branch=master

.. |coveralls| image:: https://coveralls.io/repos/ionelmc/python-lazy-object-proxy/badge.svg?branch=master&service=github
    :alt: Coverage Status
    :target: https://coveralls.io/r/ionelmc/python-lazy-object-proxy

.. |codecov| image:: https://codecov.io/gh/ionelmc/python-lazy-object-proxy/branch/master/graphs/badge.svg?branch=master
    :alt: Coverage Status
    :target: https://codecov.io/github/ionelmc/python-lazy-object-proxy

.. |version| image:: https://img.shields.io/pypi/v/lazy-object-proxy.svg
    :alt: PyPI Package latest release
    :target: https://pypi.org/project/lazy-object-proxy

.. |wheel| image:: https://img.shields.io/pypi/wheel/lazy-object-proxy.svg
    :alt: PyPI Wheel
    :target: https://pypi.org/project/lazy-object-proxy

.. |supported-versions| image:: https://img.shields.io/pypi/pyversions/lazy-object-proxy.svg
    :alt: Supported versions
    :target: https://pypi.org/project/lazy-object-proxy

.. |supported-implementations| image:: https://img.shields.io/pypi/implementation/lazy-object-proxy.svg
    :alt: Supported implementations
    :target: https://pypi.org/project/lazy-object-proxy

.. |commits-since| image:: https://img.shields.io/github/commits-since/ionelmc/python-lazy-object-proxy/v1.6.0.svg
    :alt: Commits since latest release
    :target: https://github.com/ionelmc/python-lazy-object-proxy/compare/v1.5.2...master



.. end-badges

A fast and thorough lazy object proxy.

* Free software: BSD 2-Clause License

Note that this is based on `wrapt`_'s ObjectProxy with one big change: it calls a function the first time the proxy object is
used, while `wrapt.ObjectProxy` just forwards the method calls to the target object.

In other words, you use `lazy-object-proxy` when you only have the object way later and you use `wrapt.ObjectProxy` when you
want to override few methods (by subclassing) and forward everything else to the target object.

Example::

    import lazy_object_proxy

    def expensive_func():
        from time import sleep
        print('starting calculation')
        # just as example for a very slow computation
        sleep(2)
        print('finished calculation')
        # return the result of the calculation
        return 10

    obj = lazy_object_proxy.Proxy(expensive_func)
    # function is called only when object is actually used
    print(obj)  # now expensive_func is called

    print(obj)  # the result without calling the expensive_func

Installation
============

::

    pip install lazy-object-proxy

Documentation
=============

https://python-lazy-object-proxy.readthedocs.io/

Development
===========

To run all the tests run::

    tox

Acknowledgements
================

This project is based on some code from `wrapt`_ as you can see in the git history.

.. _wrapt: https://github.com/GrahamDumpleton/wrapt
