About
=====

This module is intended to have a similar interface as the python
struct module, but working on bits instead of primitive data types
(char, int, ...).

Project homepage: https://github.com/eerimoq/bitstruct

Documentation: https://bitstruct.readthedocs.io

Installation
============

.. code-block:: python

   pip install bitstruct

Performance
===========

Parts of this package has been re-implemented in C for faster pack and
unpack operations. There are two independent C implementations;
`bitstruct.c`, which is part of this package, and the standalone
package `cbitstruct`_. These implementations are only available in
CPython 3, and must be explicitly imported. By default the pure Python
implementation is used.

To use `bitstruct.c`, do ``import bitstruct.c as bitstruct``.

To use `cbitstruct`_, do ``import cbitstruct as bitstruct``.

`bitstruct.c` has a few limitations compared to the pure Python
implementation:

- Integers and booleans must be 64 bits or less.

- Text and raw must be a multiple of 8 bits.

- Bit endianness and byte order are not yet supported.

- ``byteswap()`` can only swap 1, 2, 4 and 8 bytes.

See `cbitstruct`_ for its limitations.

MicroPython
===========

The C implementation has been ported to `MicroPython`_. See
`bitstruct-micropython`_ for more details.

Example usage
=============

A basic example of `packing`_ and `unpacking`_ four integers using the
format string ``'u1u3u4s16'``:

.. code-block:: python

    >>> from bitstruct import *
    >>> pack('u1u3u4s16', 1, 2, 3, -4)
    b'\xa3\xff\xfc'
    >>> unpack('u1u3u4s16', b'\xa3\xff\xfc')
    (1, 2, 3, -4)
    >>> calcsize('u1u3u4s16')
    24

An example `compiling`_ the format string once, and use it to `pack`_
and `unpack`_ data:

.. code-block:: python

    >>> import bitstruct
    >>> cf = bitstruct.compile('u1u3u4s16')
    >>> cf.pack(1, 2, 3, -4)
    b'\xa3\xff\xfc'
    >>> cf.unpack(b'\xa3\xff\xfc')
    (1, 2, 3, -4)

Use the `pack into`_ and `unpack from`_ functions to pack/unpack
values at a bit offset into the data, in this example the bit offset
is 5:

.. code-block:: python

    >>> from bitstruct import *
    >>> data = bytearray(b'\x00\x00\x00\x00')
    >>> pack_into('u1u3u4s16', data, 5, 1, 2, 3, -4)
    >>> data
    bytearray(b'\x05\x1f\xff\xe0')
    >>> unpack_from('u1u3u4s16', data, 5)
    (1, 2, 3, -4)

The unpacked values can be named by assigning them to variables or by
wrapping the result in a named tuple:

.. code-block:: python

    >>> from bitstruct import *
    >>> from collections import namedtuple
    >>> MyName = namedtuple('myname', ['a', 'b', 'c', 'd'])
    >>> unpacked = unpack('u1u3u4s16', b'\xa3\xff\xfc')
    >>> myname = MyName(*unpacked)
    >>> myname
    myname(a=1, b=2, c=3, d=-4)
    >>> myname.c
    3

Use the `pack_dict`_ and `unpack_dict`_ functions to pack/unpack
values in dictionaries:

.. code-block:: python

    >>> from bitstruct import *
    >>> names = ['a', 'b', 'c', 'd']
    >>> pack_dict('u1u3u4s16', names, {'a': 1, 'b': 2, 'c': 3, 'd': -4})
    b'\xa3\xff\xfc'
    >>> unpack_dict('u1u3u4s16', names, b'\xa3\xff\xfc')
    {'a': 1, 'b': 2, 'c': 3, 'd': -4}

An example of `packing`_ and `unpacking`_ an unsigned integer, a
signed integer, a float, a boolean, a byte string and a string:

.. code-block:: python

    >>> from bitstruct import *
    >>> pack('u5s5f32b1r13t40', 1, -1, 3.75, True, b'\xff\xff', 'hello')
    b'\x0f\xd0\x1c\x00\x00?\xffhello'
    >>> unpack('u5s5f32b1r13t40', b'\x0f\xd0\x1c\x00\x00?\xffhello')
    (1, -1, 3.75, True, b'\xff\xf8', 'hello')
    >>> calcsize('u5s5f32b1r13t40')
    96

The same format string and values as in the previous example, but
using LSB (Least Significant Bit) first instead of the default MSB
(Most Significant Bit) first:

.. code-block:: python

    >>> from bitstruct import *
    >>> pack('<u5s5f32b1r13t40', 1, -1, 3.75, True, b'\xff\xff', 'hello')
    b'\x87\xc0\x00\x03\x80\xbf\xff\xf666\xa6\x16'
    >>> unpack('<u5s5f32b1r13t40', b'\x87\xc0\x00\x03\x80\xbf\xff\xf666\xa6\x16')
    (1, -1, 3.75, True, b'\xff\xf8', 'hello')
    >>> calcsize('<u5s5f32b1r13t40')
    96

An example of `unpacking`_ values from a hexstring and a binary file:

.. code-block:: python

    >>> from bitstruct import *
    >>> from binascii import unhexlify
    >>> unpack('s17s13r24', unhexlify('0123456789abcdef'))
    (582, -3751, b'\xe2j\xf3')
    >>> with open("test.bin", "rb") as fin:
    ...     unpack('s17s13r24', fin.read(8))
    ...
    ...
    (582, -3751, b'\xe2j\xf3')

Change endianness of the data with `byteswap`_, and then unpack the
values:

.. code-block:: python

    >>> from bitstruct import *
    >>> packed = pack('u1u3u4s16', 1, 2, 3, 1)
    >>> unpack('u1u3u4s16', byteswap('12', packed))
    (1, 2, 3, 256)

A basic example of `packing`_ and `unpacking`_ four integers using the
format string ``'u1u3u4s16'`` using the C implementation:

.. code-block:: python

    >>> from bitstruct.c import *
    >>> pack('u1u3u4s16', 1, 2, 3, -4)
    b'\xa3\xff\xfc'
    >>> unpack('u1u3u4s16', b'\xa3\xff\xfc')
    (1, 2, 3, -4)

Contributing
============

#. Fork the repository.

#. Install prerequisites.

   .. code-block:: text

      pip install -r requirements.txt

#. Implement the new feature or bug fix.

#. Implement test case(s) to ensure that future changes do not break
   legacy.

#. Run the tests.

   .. code-block:: text

      make test

#. Create a pull request.

.. _packing: http://bitstruct.readthedocs.io/en/latest/#bitstruct.pack

.. _unpacking: http://bitstruct.readthedocs.io/en/latest/#bitstruct.unpack

.. _pack: http://bitstruct.readthedocs.io/en/latest/#bitstruct.CompiledFormat.pack

.. _unpack: http://bitstruct.readthedocs.io/en/latest/#bitstruct.CompiledFormat.unpack

.. _pack into: http://bitstruct.readthedocs.io/en/latest/#bitstruct.pack_into

.. _unpack from: http://bitstruct.readthedocs.io/en/latest/#bitstruct.unpack_from

.. _pack_dict: http://bitstruct.readthedocs.io/en/latest/#bitstruct.pack_dict

.. _unpack_dict: http://bitstruct.readthedocs.io/en/latest/#bitstruct.unpack_dict

.. _byteswap: http://bitstruct.readthedocs.io/en/latest/#bitstruct.byteswap

.. _compiling: http://bitstruct.readthedocs.io/en/latest/#bitstruct.compile

.. _cbitstruct: https://github.com/qchateau/cbitstruct

.. _MicroPython: https://github.com/micropython/micropython

.. _bitstruct-micropython: https://github.com/peterzuger/bitstruct-micropython
