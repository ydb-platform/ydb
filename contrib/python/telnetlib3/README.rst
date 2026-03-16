.. image:: https://img.shields.io/pypi/v/telnetlib3.svg
    :alt: Latest Version
    :target: https://pypi.python.org/pypi/telnetlib3

.. image:: https://img.shields.io/pypi/dm/telnetlib3.svg?logo=pypi
    :alt: Downloads
    :target: https://pypi.python.org/pypi/telnetlib3

.. image:: https://codecov.io/gh/jquast/telnetlib3/branch/master/graph/badge.svg
    :alt: codecov.io Code Coverage
    :target: https://codecov.io/gh/jquast/telnetlib3/

.. image:: https://img.shields.io/badge/Linux-yes-success?logo=linux
    :alt: Linux supported
    :target: https://telnetlib3.readthedocs.io/

.. image:: https://img.shields.io/badge/Windows-yes-success?logo=windows
    :alt: Windows supported
    :target: https://telnetlib3.readthedocs.io/

.. image:: https://img.shields.io/badge/MacOS-yes-success?logo=apple
    :alt: MacOS supported
    :target: https://telnetlib3.readthedocs.io/

.. image:: https://img.shields.io/badge/BSD-yes-success?logo=freebsd
    :alt: BSD supported
    :target: https://telnetlib3.readthedocs.io/

Introduction
============

``telnetlib3`` is a feature-rich Telnet Server, Client, and Protocol library
for Python 3.9 and newer.

This library supports both modern asyncio_ *and* legacy `Blocking API`_.

The python telnetlib.py_ module removed by Python 3.13 is also re-distributed as-is, as a backport.

See the `Guidebook`_ for examples and the `API documentation`_.

Command-line Utilities
----------------------

The CLI utility ``telnetlib3-client`` is provided for connecting to servers and
``telnetlib3-server`` for hosting a server.

Both tools accept the argument ``--shell=my_module.fn_shell`` describing a python module path to a
function of signature ``async def shell(reader, writer)``.  The server also provides ``--pty-exec``
argument to host stand-alone programs.

::

    # telnet to utf8 roguelike server
    telnetlib3-client nethack.alt.org

    # or bbs,
    telnetlib3-client xibalba.l33t.codes 44510

    # automatic script communicates with a server
    telnetlib3-client --shell bin.client_wargame.shell 1984.ws 666

    # run a server bound with the default shell bound to 127.0.0.1 6023
    telnetlib3-server

    # or custom ip, port and shell
    telnetlib3-server 0.0.0.0 1984 --shell=bin.server_wargame.shell

    # host an external program with a pseudo-terminal (raw mode is default)
    telnetlib3-server --pty-exec /bin/bash -- --login

    # or host a program in linemode,
    telnetlib3-server --pty-exec /bin/bc --line-mode

There are also two fingerprinting CLIs, ``telnetlib3-fingerprint`` and
``telnetlib3-fingerprint-server``::

    # host a server, wait for clients to connect and fingerprint them,
    telnetlib3-fingerprint-server

    # report fingerprint of the telnet server on 1984.ws
    telnetlib3-fingerprint 1984.ws

Encoding
~~~~~~~~

The default encoding is the system locale, usually UTF-8, and, without negotiation of BINARY
transmission, all Telnet protocol text *should* be limited to ASCII text, by strict compliance of
Telnet.  Further, the encoding used *should* be negotiated by CHARSET.

When these conditions are true, telnetlib3-server and telnetlib3-client allow connections of any
encoding supporting by the python language, and additionally specially ``ATASCII`` and ``PETSCII``
encodings.  Any server capable of negotiating CHARSET or LANG through NEW_ENVIRON is also presumed
to support BINARY.

From a February 2026 `census of MUDs <https://muds.modem.xyz>`_ and `BBSs servers
<https://bbs.modem.xyz>`_:

- 2.8% of MUDs support bi-directional CHARSET
- 0.5% of BBSs support bi-directional CHARSET.
- 18.4% of BBSs support BINARY.
- 3.2% of MUDs support BINARY.

For this reason, it is often required to specify the encoding, eg.!

    telnetlib3-client --encoding=cp437 20forbeers.com 1337

Raw Mode
~~~~~~~~

Some telnet servers, especially BBS systems or those designed for serial transmission but are
connected to a TCP socket without any telnet negotiation may require "raw" mode argument::

    telnetlib3-client --raw-mode area52.tk 5200 --encoding=atascii

Asyncio Protocol
----------------

The core protocol and CLI utilities are written using an `Asyncio Interface`_.

Blocking API
------------

A Synchronous interface, modeled after telnetlib.py_ (client) and miniboa_ (server), with various
enhancements in protocol negotiation is also provided.  See `sync API documentation`_ for more.

Legacy telnetlib
----------------

This library contains an *unadulterated copy* of Python 3.12's telnetlib.py_,
from the standard library before it was removed in Python 3.13.

To migrate code, change import statements:

.. code-block:: python

    # OLD imports:
    import telnetlib

    # NEW imports:
    import telnetlib3

This library *also* provides an additional client (and server) API through a similar interface but
offering more advanced negotiation features and options.  See `sync API documentation`_ for more.

Quick Example
=============

A simple telnet server:

.. code-block:: python

    import asyncio
    import telnetlib3

    async def shell(reader, writer):
        writer.write('\r\nWould you like to play a game? ')
        inp = await reader.readline()
        if inp:
            writer.echo(inp)
            writer.write('\r\nThey say the only way to win '
                         'is to not play at all.\r\n')
            await writer.drain()
        writer.close()

    async def main():
        server = await telnetlib3.create_server(port=6023, shell=shell)
        await server.wait_closed()

    asyncio.run(main())

A client that connects and plays the game:

.. code-block:: python

    import asyncio
    import telnetlib3

    async def shell(reader, writer):
        while True:
            output = await reader.read(1024)
            if not output:
                break
            if '?' in output:
                writer.write('y\r\n')
            print(output, end='', flush=True)
        print()

    async def main():
        reader, writer = await telnetlib3.open_connection('localhost', 6023)
        await shell(reader, writer)

    asyncio.run(main())

More examples are available in the `Guidebook`_ and the `bin/`_ directory of the repository.

Features
--------

The following RFC specifications are implemented:

* `rfc-727`_, "Telnet Logout Option," Apr 1977.
* `rfc-779`_, "Telnet Send-Location Option", Apr 1981.
* `rfc-854`_, "Telnet Protocol Specification", May 1983.
* `rfc-855`_, "Telnet Option Specifications", May 1983.
* `rfc-856`_, "Telnet Binary Transmission", May 1983.
* `rfc-857`_, "Telnet Echo Option", May 1983.
* `rfc-858`_, "Telnet Suppress Go Ahead Option", May 1983.
* `rfc-859`_, "Telnet Status Option", May 1983.
* `rfc-860`_, "Telnet Timing Mark Option", May 1983.
* `rfc-885`_, "Telnet End of Record Option", Dec 1983.
* `rfc-930`_, "Telnet Terminal Type Option", Jan 1984.
* `rfc-1073`_, "Telnet Window Size Option", Oct 1988.
* `rfc-1079`_, "Telnet Terminal Speed Option", Dec 1988.
* `rfc-1091`_, "Telnet Terminal-Type Option", Feb 1989.
* `rfc-1096`_, "Telnet X Display Location Option", Mar 1989.
* `rfc-1123`_, "Requirements for Internet Hosts", Oct 1989.
* `rfc-1184`_, "Telnet Linemode Option (extended options)", Oct 1990.
* `rfc-1372`_, "Telnet Remote Flow Control Option", Oct 1992.
* `rfc-1408`_, "Telnet Environment Option", Jan 1993.
* `rfc-1571`_, "Telnet Environment Option Interoperability Issues", Jan 1994.
* `rfc-1572`_, "Telnet Environment Option", Jan 1994.
* `rfc-2066`_, "Telnet Charset Option", Jan 1997.

.. _rfc-727: https://www.rfc-editor.org/rfc/rfc727.txt
.. _rfc-779: https://www.rfc-editor.org/rfc/rfc779.txt
.. _rfc-854: https://www.rfc-editor.org/rfc/rfc854.txt
.. _rfc-855: https://www.rfc-editor.org/rfc/rfc855.txt
.. _rfc-856: https://www.rfc-editor.org/rfc/rfc856.txt
.. _rfc-857: https://www.rfc-editor.org/rfc/rfc857.txt
.. _rfc-858: https://www.rfc-editor.org/rfc/rfc858.txt
.. _rfc-859: https://www.rfc-editor.org/rfc/rfc859.txt
.. _rfc-860: https://www.rfc-editor.org/rfc/rfc860.txt
.. _rfc-885: https://www.rfc-editor.org/rfc/rfc885.txt
.. _rfc-930: https://www.rfc-editor.org/rfc/rfc930.txt
.. _rfc-1073: https://www.rfc-editor.org/rfc/rfc1073.txt
.. _rfc-1079: https://www.rfc-editor.org/rfc/rfc1079.txt
.. _rfc-1091: https://www.rfc-editor.org/rfc/rfc1091.txt
.. _rfc-1096: https://www.rfc-editor.org/rfc/rfc1096.txt
.. _rfc-1123: https://www.rfc-editor.org/rfc/rfc1123.txt
.. _rfc-1184: https://www.rfc-editor.org/rfc/rfc1184.txt
.. _rfc-1372: https://www.rfc-editor.org/rfc/rfc1372.txt
.. _rfc-1408: https://www.rfc-editor.org/rfc/rfc1408.txt
.. _rfc-1571: https://www.rfc-editor.org/rfc/rfc1571.txt
.. _rfc-1572: https://www.rfc-editor.org/rfc/rfc1572.txt
.. _rfc-2066: https://www.rfc-editor.org/rfc/rfc2066.txt
.. _`bin/`: https://github.com/jquast/telnetlib3/tree/master/bin
.. _telnetlib.py: https://docs.python.org/3.12/library/telnetlib.html
.. _Asyncio Interface: https://telnetlib3.readthedocs.io/en/latest/guidebook.html#asyncio-interface
.. _Blocking API: https://telnetlib3.readthedocs.io/en/latest/guidebook.html#blocking-interface
.. _Guidebook: https://telnetlib3.readthedocs.io/en/latest/guidebook.html
.. _API documentation: https://telnetlib3.readthedocs.io/en/latest/api.html
.. _sync API documentation: https://telnetlib3.readthedocs.io/en/latest/api/sync.html
.. _miniboa: https://github.com/shmup/miniboa
.. _asyncio: https://docs.python.org/3/library/asyncio.html

Further Reading
---------------

Further documentation available at https://telnetlib3.readthedocs.io/
