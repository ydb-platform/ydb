pymemcache
==========

.. image:: https://img.shields.io/pypi/v/pymemcache.svg
    :target: https://pypi.python.org/pypi/pymemcache

.. image:: https://readthedocs.org/projects/pymemcache/badge/?version=master
        :target: https://pymemcache.readthedocs.io/en/latest/
        :alt: Master Documentation Status

A comprehensive, fast, pure-Python memcached client.

pymemcache supports the following features:

* Complete implementation of the memcached text protocol.
* Connections using UNIX sockets, or TCP over IPv4 or IPv6.
* Configurable timeouts for socket connect and send/recv calls.
* Access to the "noreply" flag, which can significantly increase the speed of writes.
* Flexible, modular and simple approach to serialization and deserialization.
* The (optional) ability to treat network and memcached errors as cache misses.

Installing pymemcache
=====================

Install from pip:

.. code-block:: bash

  pip install pymemcache

For development, clone from github and run the tests:

.. code-block:: bash

    git clone https://github.com/pinterest/pymemcache.git
    cd pymemcache

Run the tests (make sure you have a local memcached server running):

.. code-block:: bash

    tox

Usage
=====

See the documentation here: https://pymemcache.readthedocs.io/en/latest/

Django
------

Since version 3.2, Django has included a pymemcache-based cache backend.
See `its documentation 
<https://docs.djangoproject.com/en/stable/topics/cache/#memcached>`__.

On older Django versions, you can use
`django-pymemcache <https://github.com/django-pymemcache/django-pymemcache>`_.

Comparison with Other Libraries
===============================

pylibmc
-------

The pylibmc library is a wrapper around libmemcached, implemented in C. It is
fast, implements consistent hashing, the full memcached protocol and timeouts.
It does not provide access to the "noreply" flag. It also isn't pure Python,
so using it with libraries like gevent is out of the question, and its
dependency on libmemcached poses challenges (e.g., it must be built against
the same version of libmemcached that it will use at runtime).

python-memcached
----------------

The python-memcached library implements the entire memcached text protocol, has
a single timeout for all socket calls and has a flexible approach to
serialization and deserialization. It is also written entirely in Python, so
it works well with libraries like gevent. However, it is tied to using thread
locals, doesn't implement "noreply", can't treat errors as cache misses and is
slower than both pylibmc and pymemcache. It is also tied to a specific method
for handling clusters of memcached servers.

memcache_client
---------------

The team at mixpanel put together a pure Python memcached client as well. It
has more fine grained support for socket timeouts, only connects to a single
host. However, it doesn't support most of the memcached API (just get, set,
delete and stats), doesn't support "noreply", has no serialization or
deserialization support and can't treat errors as cache misses.

External Links
==============

The memcached text protocol reference page:
  https://github.com/memcached/memcached/blob/master/doc/protocol.txt

The python-memcached library (another pure-Python library):
  https://github.com/linsomniac/python-memcached

Mixpanel's Blog post about their memcached client for Python:
  https://engineering.mixpanel.com/we-went-down-so-we-wrote-a-better-pure-python-memcache-client-b409a9fe07a9

Mixpanel's pure Python memcached client:
  https://github.com/mixpanel/memcache_client
  
Bye-bye python-memcached, hello pymemcache (migration guide)
  https://jugmac00.github.io/blog/bye-bye-python-memcached-hello-pymemcache/

Credits
=======

* `Charles Gordon <http://github.com/cgordon>`_
* `Dave Dash <http://github.com/davedash>`_
* `Dan Crosta <http://github.com/dcrosta>`_
* `Julian Berman <http://github.com/Julian>`_
* `Mark Shirley <http://github.com/maspwr>`_
* `Tim Bart <http://github.com/pims>`_
* `Thomas Orozco <http://github.com/krallin>`_
* `Marc Abramowitz <http://github.com/msabramo>`_
* `Marc-Andre Courtois <http://github.com/mcourtois>`_
* `Julien Danjou <http://github.com/jd>`_
* `INADA Naoki <http://github.com/methane>`_
* `James Socol <http://github.com/jsocol>`_
* `Joshua Harlow <http://github.com/harlowja>`_
* `John Anderson <http://github.com/sontek>`_
* `Adam Chainz <http://github.com/adamchainz>`_
* `Ernest W. Durbin III <https://github.com/ewdurbin>`_
* `Remco van Oosterhout <https://github.com/Vhab>`_
* `Nicholas Charriere <https://github.com/nichochar>`_
* `Joe Gordon <https://github.com/jogo>`_
* `Jon Parise <https://github.com/jparise>`_
* `Stephen Rosen <https://github.com/sirosen>`_
* `Feras Alazzeh <https://github.com/FerasAlazzeh>`_
* `Moisés Guimarães de Medeiros <https://github.com/moisesguimaraes>`_
* `Nick Pope <https://github.com/ngnpope>`_
* `Hervé Beraud <https://github.com/4383>`_
* `Martin Jørgensen <https://github.com/martinnj>`_

We're Hiring!
=============
Are you really excited about open-source? Or great software engineering?
Pinterest is `hiring <https://careers.pinterest.com/>`_!
