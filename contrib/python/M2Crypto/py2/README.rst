========
M2Crypto
========

:Maintainer: Matěj Cepl
:Web-Site: https://gitlab.com/m2crypto/m2crypto
:Documentation: https://m2crypto.readthedocs.io/
:Email list: m2crypto@lists.redcrew.org or http://redcrew.org/mailman/listinfo/m2crypto

M2Crypto = Python + OpenSSL + SWIG
----------------------------------

M2Crypto is a crypto and SSL toolkit for Python.

M2 stands for "me, too!"

M2Crypto comes with the following:

- **RSA**, **DSA**, **DH**, **HMACs**, **message digests**,
  **symmetric ciphers** including **AES**,

- **TLS** functionality to implement **clients and servers**.

- **Example SSL client and server programs**, which are variously
  **threading**, **forking** or based on **non-blocking socket IO**.

- **HTTPS** extensions to Python's **httplib, urllib and xmlrpclib**.

- Unforgeable HMAC'ing **AuthCookies** for **web session management**.

- **FTP/TLS** client and server.

- **S/MIME v2**.

- **ZSmime**: An S/MIME messenger for **Zope**.

We care a lot about stable API and all Python methods should be
preserved, note however that ``m2.`` namespace is considered internal to
the library and it doesn't have to be preserved. If however some change
to it breaks your app, let us know and we will try to make things
working for you.

- And much more.

M2Crypto is released under a very liberal MIT licence. See
LICENCE for details.

To install, see the file INSTALL.

Look at the tests and demos for example use. Recommended reading before
deploying in production is "Network Security with OpenSSL" by John Viega,
Matt Messier and Pravir Chandra, ISBN 059600270X.

Note these caveats:

- Possible memory leaks, because some objects need to be freed on the
  Python side and other objects on the C side, and these may change
  between OpenSSL versions. (Multiple free's lead to crashes very
  quickly, so these should be relatively rare.)

- No memory locking/clearing for keys, passphrases, etc. because AFAIK
  Python does not provide the features needed. On the C (OpenSSL) side
  things are cleared when the Python objects are deleted.

Have fun! Your feedback is welcome.
