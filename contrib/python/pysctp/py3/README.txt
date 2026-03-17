PySCTP - SCTP bindings for Python
---------------------------------

Elvis Pfützenreuter 
Instituto Nokia de Tecnologia (http://www.indt.org.br)
epx __AT__ epx.com.br

Philippe Langlois
P1 Security (http://www.p1sec.com)
phil __AT__ p1sec.com

======================================================================
INSTALL

sudo python setup.py install

* to see what this is going to install without actually doing it:
python setup.py install --dry-run

* to just build and not install:
python setup.py build

In case you want to install the module explicitely for Python 2 or 3,
just replace _python_ by _python2_ / _python3_ in the commands above.

======================================================================
DEPENDENCIES:

You can automatically install dependencies for Debian/Ubuntu:
make installdeps

Otherwise, necessary would be e.g. on Ubuntu: libsctp-dev and python-dev 
(python2-dev or python3-dev for an explicit version of Python)

Support for Mac OSX is not tested, but should be doable through the SCTP Network
Kernel Extension (NKE) available at:
https://github.com/sctplab/SCTP_NKE_HighSierra

======================================================================
INTRODUCTION

PySCTP gives access to the SCTP transport protocol from Python language.
It extends the traditional socket interface, allowing
SCTP sockets to be used in most situations where a TCP or UDP socket
would work, while preserving the unique characteristics of the protocol.

For more information about SCTP, go to http://www.sctp.org or RFC 4960.
For discussion, sources, bugs, go to http://github.com/p1sec/pysctp

In a nutshell, PySCTP can be used as follows:

---------

import socket
import sctp

sk = sctp.sctpsocket_tcp(socket.AF_INET)
sk.connect(("10.0.1.1", 36413))

... most socket operations work for SCTP too ...

sk.close()

---------

The autotest programs (e.g. test_local_cnx.py) are actually good examples of 
pysctp usage.

The BSD/Sockets SCTP extensions are defined by an IETF draft
(draft-ietf-tsvwg-sctpsocket-10.txt) and PySCTP tries to map those
extensions very closely. So, to really take the most advantage of
SCTP and PySCTP, you must understand how the API works. You can
find advice about it in the the draft itself (not incredibly easy
to understand though), as well the 3rd edition of Unix Network 
Programming.


======================================================================
DESCRIPTION

1) The "sctp" module

The "sctp" module is the Python side of the bindings. The docstrings
of every class and method can give good advice of functions, but the
highlights are:

* sctpsocket is the root class for SCTP sockets, that ought not be used
  directly by the users. It does *not* inherit directly from Python
  standard socket; instead it *contains* a socket. That design was
  followed mostly because UDP-style sockets can be "peeled off" and 
  return TCP-style sockets. 

  sctpsocket delegates unknown methods to the socket. This ensures that
  methods like close(), bind(), read(), select() etc. will work as expected.
  If the real socket is really needed, it can be obtained with
  sctpsocket.sock().

* As said, "Normal" socket calls like open(), bind(), close() etc. 
  can be used on SCTP sockets because they are delegated to the
  Python socket. 

* Users will normally use the sctpsocket_tcp (TCP style) and sctpsocket_udp
  (UDP style) classes. Some calls that are implemented in sctpsocket but 
  do not make sense in a particular style are rendered invalid in each
  class (e.g. peeloff() in TCP-style sockets).

2) The "_sctp" module

This is the C side of the bindings, that provides the "glue" between
Python and the C API. The regular PySCTP user should not need to get 
into this, but power users and developers may be interested in it. 

The interface between Python and C is designed to be as simple as
possible. In particular, no object is created in C side, just 
simple types (strings, integers, lists, tuples and dictionaries).

The translation to/from complex objects is done entirely in Python.
It avoids that _sctp depends on sctp.

NOTE: it all has been tested agains lksctp-utils 1.0.1 and kernel
2.6.10, that come with Ubuntu Hoary. Some newer calls like connectx()
depend of testing on a newer environment to be implemented.


======================================================================
License

This module is licensed under the LGPL license.

======================================================================
Credits

Elvis Pfützenreuter <elvis.pfutzenreuter __AT__ indt.org.br>
Philippe Langlois <phil __AT__ p1sec.com>
Casimiro Daniel NPRI <CasimiroD  __AT__ npt.nuwc.navy.mil> - patch for new SCTP_* constants

