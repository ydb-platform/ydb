About
=====

Mangling of various file formats that conveys binary information
(Motorola S-Record, Intel HEX, TI-TXT, Verilog VMEM, ELF and binary
files).

Project homepage: https://github.com/eerimoq/bincopy

Documentation: https://bincopy.readthedocs.io

Installation
============

.. code-block:: python

    pip install bincopy

Example usage
=============

Scripting
---------

A basic example converting from Intel HEX to Intel HEX, SREC, binary,
array and hexdump formats:

.. code-block:: pycon

    >>> import bincopy
    >>> f = bincopy.BinFile("tests/files/in.hex")
    >>> print(f.as_ihex())
    :20010000214601360121470136007EFE09D219012146017E17C20001FF5F16002148011979
    :20012000194E79234623965778239EDA3F01B2CA3F0156702B5E712B722B7321460134219F
    :00000001FF

    >>> print(f.as_srec())
    S32500000100214601360121470136007EFE09D219012146017E17C20001FF5F16002148011973
    S32500000120194E79234623965778239EDA3F01B2CA3F0156702B5E712B722B73214601342199
    S5030002FA

    >>> print(f.as_ti_txt())
    @0100
    21 46 01 36 01 21 47 01 36 00 7E FE 09 D2 19 01
    21 46 01 7E 17 C2 00 01 FF 5F 16 00 21 48 01 19
    19 4E 79 23 46 23 96 57 78 23 9E DA 3F 01 B2 CA
    3F 01 56 70 2B 5E 71 2B 72 2B 73 21 46 01 34 21
    q

    >>> print(f.as_verilog_vmem())
    @00000100 21 46 01 36 01 21 47 01 36 00 7E FE 09 D2 19 01 21 46 01 7E 17 C2 00 01 FF 5F 16 00 21 48 01 19
    @00000120 19 4E 79 23 46 23 96 57 78 23 9E DA 3F 01 B2 CA 3F 01 56 70 2B 5E 71 2B 72 2B 73 21 46 01 34 21

    >>> f.as_binary()
    bytearray(b'!F\x016\x01!G\x016\x00~\xfe\t\xd2\x19\x01!F\x01~\x17\xc2\x00\x01
    \xff_\x16\x00!H\x01\x19\x19Ny#F#\x96Wx#\x9e\xda?\x01\xb2\xca?\x01Vp+^q+r+s!
    F\x014!')
    >>> list(f.segments)
    [Segment(address=256, data=bytearray(b'!F\x016\x01!G\x016\x00~\xfe\t\xd2\x19\x01
    !F\x01~\x17\xc2\x00\x01\xff_\x16\x00!H\x01\x19\x19Ny#F#\x96Wx#\x9e\xda?\x01
    \xb2\xca?\x01Vp+^q+r+s!F\x014!'))]
    >>> f.minimum_address
    256
    >>> f.maximum_address
    320
    >>> len(f)
    64
    >>> f[f.minimum_address]
    33
    >>> f[f.minimum_address:f.minimum_address + 1]
    bytearray(b'!')

See the `test suite`_ for additional examples.

Command line tool
-----------------

The info subcommand
^^^^^^^^^^^^^^^^^^^

Print general information about given binary format file(s).

.. code-block:: text

   $ bincopy info tests/files/in.hex
   File:                    tests/files/in.hex
   Data ranges:

       0x00000100 - 0x00000140 (64 bytes)

   Data ratio:              100.0 %
   Layout:

       0x100                                                      0x140
       ================================================================

The convert subcommand
^^^^^^^^^^^^^^^^^^^^^^

Convert file(s) from one format to another.

.. code-block:: text

   $ bincopy convert -i ihex -o srec tests/files/in.hex -
   S32500000100214601360121470136007EFE09D219012146017E17C20001FF5F16002148011973
   S32500000120194E79234623965778239EDA3F01B2CA3F0156702B5E712B722B73214601342199
   S5030002FA
   $ bincopy convert -i binary -o hexdump tests/files/in.hex -
   00000000  3a 32 30 30 31 30 30 30  30 32 31 34 36 30 31 33  |:200100002146013|
   00000010  36 30 31 32 31 34 37 30  31 33 36 30 30 37 45 46  |60121470136007EF|
   00000020  45 30 39 44 32 31 39 30  31 32 31 34 36 30 31 37  |E09D219012146017|
   00000030  45 31 37 43 32 30 30 30  31 46 46 35 46 31 36 30  |E17C20001FF5F160|
   00000040  30 32 31 34 38 30 31 31  39 37 39 0a 3a 32 30 30  |02148011979.:200|
   00000050  31 32 30 30 30 31 39 34  45 37 39 32 33 34 36 32  |12000194E7923462|
   00000060  33 39 36 35 37 37 38 32  33 39 45 44 41 33 46 30  |3965778239EDA3F0|
   00000070  31 42 32 43 41 33 46 30  31 35 36 37 30 32 42 35  |1B2CA3F0156702B5|
   00000080  45 37 31 32 42 37 32 32  42 37 33 32 31 34 36 30  |E712B722B7321460|
   00000090  31 33 34 32 31 39 46 0a  3a 30 30 30 30 30 30 30  |134219F.:0000000|
   000000a0  31 46 46 0a                                       |1FF.            |

Concatenate two or more files.

.. code-block:: text

   $ bincopy convert -o srec tests/files/in.s19 tests/files/convert.s19 -
   S00F000068656C6C6F202020202000003C
   S325000000007C0802A6900100049421FFF07C6C1B787C8C23783C600000386300004BFFFFE5F2
   S32500000020398000007D83637880010014382100107C0803A64E80002048656C6C6F20776F13
   S30B00000040726C642E0A003A
   S32500000100214601360121470136007EFE09D219012146017E17C20001FF5F16002148011973
   S32500000120194E79234623965778239EDA3F01B2CA3F0156702B5E712B722B73214601342199
   S5030005F7
   S70500000000FA

The pretty subcommand
^^^^^^^^^^^^^^^^^^^^^

Easy to read Motorola S-Record, Intel HEX and TI TXT files with the
pretty subcommand.

.. image:: https://github.com/eerimoq/bincopy/raw/master/docs/pretty-s19.png

.. image:: https://github.com/eerimoq/bincopy/raw/master/docs/pretty-hex.png

.. image:: https://github.com/eerimoq/bincopy/raw/master/docs/pretty-ti-txt.png

The fill subcommand
^^^^^^^^^^^^^^^^^^^

Fill empty space between segments. Use ``--max-words`` to only fill
gaps smaller than given size.

.. code-block:: text

   $ bincopy info tests/files/in_exclude_2_4.s19 | grep byte
       0x00000000 - 0x00000002 (2 bytes)
       0x00000004 - 0x00000046 (66 bytes)
   $ bincopy fill tests/files/in_exclude_2_4.s19 filled.s19
   $ bincopy info filled.s19 | grep byte
       0x00000000 - 0x00000046 (70 bytes)

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

Similar projects
================

These projects provides features similar to bincopy:

- `SRecord`_ (``srec_cat`` and ``srec_info``)

- `IntelHex`_ (Python IntelHex library)

- `objutils`_ (Process HEX files in Python)

.. _test suite: https://github.com/eerimoq/bincopy/blob/master/tests/test_bincopy.py

.. _SRecord: http://srecord.sourceforge.net/

.. _IntelHex: https://github.com/python-intelhex/intelhex

.. _objutils: https://github.com/christoph2/objutils
