greenify_
=========

|build| |status| |pypiv| |pyversions| |wheel| |license|

greenify_ can make Python extension modules having network operations in C
code to be compatible with gevent_.

greenify_ uses the Dynamic Function Redirecting technique same as ELF-Hook_
to patch blocking network operations at runtime, without the need modify
the original modules.

Currently greenify_ only supports ELF format modules, and is tested on Linux.


Install from source
-------------------

``greenify`` module is installed using setuptools or pip::

  python setup.py install

or::

  pip install greenify

Usage
-----

1. Active greenify_ before initiate environment::

    import greenify
    greenify.greenify()

2. Make sure the dynamic module(e.g. libmemcached) is patched before using::

    assert greenify.patch_lib('/usr/lib/libmemcached.so')

3. Import and use the corresponding module, which is now gevent_ compatible.

Thread Safety
-------------

Once activated, the green C functions will, on potentially blocking operation,
pass control to gevent's main event loop, which may switch to other ready
greenlet which is also in one of the green functions.  So, make sure your C
code can handle this kind of execution pause and resume.  A thread safe
program usually is ready for greenify, but remember that all the switches
happen in a single thread.


License
-------

greenify_ is written and maintained by `douban`_ and is licensed under New BSD license.


.. _gevent: http://www.gevent.org
.. _greenify: https://github.com/douban/greenify
.. _douban: http://www.douban.com
.. _ELF-Hook: https://github.com/shoumikhin/ELF-Hook

.. |build| image:: https://github.com/douban/greenify/actions/workflows/test.yml/badge.svg
   :target: https://github.com/douban/greenify/actions/workflows/test.yml

.. |pypiv| image:: https://img.shields.io/pypi/v/greenify
   :target: https://pypi.org/project/greenify/

.. |status| image:: https://img.shields.io/pypi/status/greenify
.. |pyversions| image:: https://img.shields.io/pypi/pyversions/greenify
.. |wheel| image:: https://img.shields.io/pypi/wheel/greenify
.. |license| image:: https://img.shields.io/pypi/l/greenify?color=blue
