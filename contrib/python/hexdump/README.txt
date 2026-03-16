
What is it about?

* *dump* binary to hex and *restore* it back
* Linux / Windows / OS X
* Python 2/3
* library and command line tool


command line
============
There are three ways to execute hexdump.py from command line::

   $ python hexdump.py
   $ python hexdump-3.2.zip

   # after installing with `pip install hexdump`
   $ python -m hexdump

Dump binary data in hex form::

   $ python -m hexdump binary.dat
   0000000000: 00 00 00 5B 68 65 78 64  75 6D 70 5D 00 00 00 00  ...[hexdump]....
   0000000010: 00 11 22 33 44 55 66 77  88 99 AA BB CC DD EE FF  .."3DUfw........

Restore binary from a saved hex dump::

   $ python -m hexdump --restore hexdump.txt > binary.dat


basic API
=========
dump(binary, size=2, sep=' ')

   Convert binary data (bytes in Python 3 and
   str in Python 2) to string like '00 DE AD BE EF'.
   `size` argument specifies length of text chunks
   and `sep` sets chunk separator.

dehex(hextext)

   Helper to convert from hex string to binary data
   stripping whitespaces from `hextext` if necessary.


advanced API: write full dumps
==============================

Python 2::

   >>> hexdump('\x00'*16)
   00000000: 00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  ................

Python 3::

   >>> hexdump('\x00'*16)
   ...
   TypeError: Abstract unicode data (expected bytes)
   >>> hexdump.hexdump(b'\x00'*16)
   00000000: 00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  ................
 
Python 3 string is a sequence of indexes in abstract unicode
table. Each index points to a symbol, which doesn't specify
its binary value. To convert symbol to binary data, you need
to lookup binary a value for in in the encoding.

Here is how the same Russian text looks when transformed from
abstract unicode integers of Python 3 to bytes in Windows-1251
encoding and to bytes in UTF-8.

   >>> message = 'интерференция'
   >>> hexdump(message.encode('windows-1251'))
   00000000: E8 ED F2 E5 F0 F4 E5 F0  E5 ED F6 E8 FF           .............
   >>> hexdump(message.encode('utf-8'))
   00000000: D0 B8 D0 BD D1 82 D0 B5  D1 80 D1 84 D0 B5 D1 80  ................
   00000010: D0 B5 D0 BD D1 86 D0 B8  D1 8F                    ..........


advanced API: restore binary data from different hexdump formats
================================================================

Python 2::

   >>> res = restore(
   ... '0010: 00 11 22 33 44 55 66 77  88 99 AA BB CC DD EE FF  .."3DUfw........')
   >>> res
   '\x00\x11"3DUfw\x88\x99\xaa\xbb\xcc\xdd\xee\xff'
   >>> type(res)
   <type 'str'>

Python 3::

   >>> res = restore(
   ... '0010: 00 11 22 33 44 55 66 77  88 99 AA BB CC DD EE FF  .."3DUfw........')
   >>> res
   b'\x00\x11"3DUfw\x88\x99\xaa\xbb\xcc\xdd\xee\xff'
   >>> type(res)
   <class 'bytes'>


run self-tests
==============
Manually::

   $ hexdump.py --test output.txt
   $ diff -u3 hextest.txt output.txt

Automatically with `tox`::

   $ tox


questions
=========
| Q: Why creating another module when there is binascii already?
| A: ``binascii.unhexlify()`` chokes on whitespaces and linefeeds.
| ``hexdump.dehex()`` doesn't have this problem.

If you have other questions, feel free to open an issue
at https://bitbucket.org/techtonik/hexdump/


ChangeLog
=========
3.3 (2015-01-22)
 * accept input from sys.stdin if "-" is specified
   for both dump and restore (issue #1)
 * new normalize_py() helper to set sys.stdout to
   binary mode on Windows

3.2 (2015-07-02)
 * hexdump is now packaged as .zip on all platforms
   (on Linux created archive was tar.gz)
 * .zip is executable! try `python hexdump-3.2.zip`
 * dump() now accepts configurable separator, patch
   by Ian Land (PR #3)

3.1 (2014-10-20)
 * implemented workaround against mysterious coding
   issue with Python 3 (see revision 51302cf)
 * fix Python 3 installs for systems where UTF-8 is
   not default (Windows), thanks to George Schizas
   (the problem was caused by reading of README.txt)

3.0 (2014-09-07)
 * remove unused int2byte() helper
 * add dehex(text) helper to convert hex string
   to binary data
 * add 'size' argument to dump() helper to specify
   length of chunks

2.0 (2014-02-02)
 * add --restore option to command line mode to get
   binary data back from hex dump
 * support saving test output with `--test logfile`
 * restore() from hex strings without spaces
 * restore() now raises TypeError if input data is
   not string
 * hexdump() and dumpgen() now don't return unicode
   strings in Python 2.x when generator is requested

1.0 (2013-12-30)
 * length of address is reduced from 10 to 8
 * hexdump() got new 'result' keyword argument, it
   can be either 'print', 'generator' or 'return'
 * actual dumping logic is now in new dumpgen()
   generator function
 * new dump(binary) function that takes binary data
   and returns string like "66 6F 72 6D 61 74"
 * new genchunks(mixed, size) function that chunks
   both sequences and file like objects

0.5 (2013-06-10)
 * hexdump is now also a command line utility (no
   restore yet)

0.4 (2013-06-09)
 * fix installation with Python 3 for non English
   versions of Windows, thanks to George Schizas

0.3 (2013-04-29)
 * fully Python 3 compatible

0.2 (2013-04-28)
 * restore() to recover binary data from a hex dump in
   native, Far Manager and Scapy text formats (others
   might work as well)
 * restore() is Python 3 compatible

0.1 (2013-04-28)
 * working hexdump() function for Python 2


Release checklist
=================

| [ ] run tests  
| [ ] update version in hexdump.py  
| [ ] update ChangeLog in README.txt from hexdump.py  
| [ ] python setup.py register sdist upload  


License
=======
Public Domain


Credits
=======
| anatoly techtonik <techtonik@gmail.com>  
| George Schizas  
| Ian Land
