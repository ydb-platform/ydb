python-twofish
==============

Bindings for the Twofish implementation by Niels Ferguson libtwofish-dev_.

Compatible with Python 2.6, 2.7 and 3.3.

The library performs a self-test at each import.

.. _libtwofish-dev: http://packages.debian.org/sid/libtwofish-dev

Installation
------------

::

  pip install twofish

Usage
-----

Create a ``twofish.Twofish`` instance with a key of length ]0, 32] and then use the ``encrypt`` and ``decrypt`` methods on 16 bytes blocks.

All values must be binary strings (``str`` on Python 2, ``bytes`` on Python 3)

**[WARNING]** this should be used in a senseful cipher mode, like CTR or CBC. If you don't know what this mean, you should probably usa a higher level library.

Example
-------

>>> from twofish import Twofish
>>> T = Twofish(b'*secret*')
>>> x = T.encrypt(b'YELLOWSUBMARINES')
>>> print(T.decrypt(x).decode())
YELLOWSUBMARINES
