aioquic
=======

.. image:: https://img.shields.io/pypi/l/aioquic.svg
   :target: https://pypi.python.org/pypi/aioquic
   :alt: License

.. image:: https://img.shields.io/pypi/v/aioquic.svg
   :target: https://pypi.python.org/pypi/aioquic
   :alt: Version

.. image:: https://img.shields.io/pypi/pyversions/aioquic.svg
   :target: https://pypi.python.org/pypi/aioquic
   :alt: Python versions

.. image:: https://github.com/aiortc/aioquic/workflows/tests/badge.svg
   :target: https://github.com/aiortc/aioquic/actions
   :alt: Tests

.. image:: https://img.shields.io/codecov/c/github/aiortc/aioquic.svg
   :target: https://codecov.io/gh/aiortc/aioquic
   :alt: Coverage

.. image:: https://readthedocs.org/projects/aioquic/badge/?version=latest
   :target: https://aioquic.readthedocs.io/
   :alt: Documentation

What is ``aioquic``?
--------------------

``aioquic`` is a library for the QUIC network protocol in Python. It features
a minimal TLS 1.3 implementation, a QUIC stack and an HTTP/3 stack.

``aioquic`` is used by Python opensource projects such as `dnspython`_,
`hypercorn`_, `mitmproxy`_ and the `Web Platform Tests`_ cross-browser test
suite. It has also been used extensively in research papers about QUIC.

To learn more about ``aioquic`` please `read the documentation`_.

Why should I use ``aioquic``?
-----------------------------

``aioquic`` has been designed to be embedded into Python client and server
libraries wishing to support QUIC and / or HTTP/3. The goal is to provide a
common codebase for Python libraries in the hope of avoiding duplicated effort.

Both the QUIC and the HTTP/3 APIs follow the "bring your own I/O" pattern,
leaving actual I/O operations to the API user. This approach has a number of
advantages including making the code testable and allowing integration with
different concurrency models.

A lot of effort has gone into writing an extensive test suite for the
``aioquic`` code to ensure best-in-class code quality, and it is regularly
`tested for interoperability`_ against other `QUIC implementations`_.

Features
--------

- minimal TLS 1.3 implementation conforming with `RFC 8446`_
- QUIC stack conforming with `RFC 9000`_ (QUIC v1) and `RFC 9369`_ (QUIC v2)
   * IPv4 and IPv6 support
   * connection migration and NAT rebinding
   * logging TLS traffic secrets
   * logging QUIC events in QLOG format
   * version negotiation conforming with `RFC 9368`_
- HTTP/3 stack conforming with `RFC 9114`_
   * server push support
   * WebSocket bootstrapping conforming with `RFC 9220`_
   * datagram support conforming with `RFC 9297`_

Installing
----------

The easiest way to install ``aioquic`` is to run:

.. code:: bash

    pip install aioquic

Building from source
--------------------

If there are no wheels for your system or if you wish to build ``aioquic``
from source you will need the OpenSSL development headers.

Linux
.....

On Debian/Ubuntu run:

.. code-block:: console

   sudo apt install libssl-dev python3-dev

On Alpine Linux run:

.. code-block:: console

   sudo apk add openssl-dev python3-dev bsd-compat-headers libffi-dev

OS X
....

On OS X run:

.. code-block:: console

   brew install openssl

You will need to set some environment variables to link against OpenSSL:

.. code-block:: console

   export CFLAGS=-I$(brew --prefix openssl)/include
   export LDFLAGS=-L$(brew --prefix openssl)/lib

Windows
.......

On Windows the easiest way to install OpenSSL is to use `Chocolatey`_.

.. code-block:: console

   choco install openssl

You will need to set some environment variables to link against OpenSSL:

.. code-block:: console

  $Env:INCLUDE = "C:\Progra~1\OpenSSL\include"
  $Env:LIB = "C:\Progra~1\OpenSSL\lib"

Running the examples
--------------------

`aioquic` comes with a number of examples illustrating various QUIC usecases.

You can browse these examples here: https://github.com/aiortc/aioquic/tree/main/examples

License
-------

``aioquic`` is released under the `BSD license`_.

.. _read the documentation: https://aioquic.readthedocs.io/en/latest/
.. _dnspython: https://github.com/rthalley/dnspython
.. _hypercorn: https://github.com/pgjones/hypercorn
.. _mitmproxy: https://github.com/mitmproxy/mitmproxy
.. _Web Platform Tests: https://github.com/web-platform-tests/wpt
.. _tested for interoperability: https://interop.seemann.io/
.. _QUIC implementations: https://github.com/quicwg/base-drafts/wiki/Implementations
.. _cryptography: https://cryptography.io/
.. _Chocolatey: https://chocolatey.org/
.. _BSD license: https://aioquic.readthedocs.io/en/latest/license.html
.. _RFC 8446: https://datatracker.ietf.org/doc/html/rfc8446
.. _RFC 9000: https://datatracker.ietf.org/doc/html/rfc9000
.. _RFC 9114: https://datatracker.ietf.org/doc/html/rfc9114
.. _RFC 9220: https://datatracker.ietf.org/doc/html/rfc9220
.. _RFC 9297: https://datatracker.ietf.org/doc/html/rfc9297
.. _RFC 9368: https://datatracker.ietf.org/doc/html/rfc9368
.. _RFC 9369: https://datatracker.ietf.org/doc/html/rfc9369
