cChardet
========

cChardet is high speed universal character encoding detector. - binding to `uchardet`_.

.. image:: https://badge.fury.io/py/cchardet.svg
   :target: https://badge.fury.io/py/cchardet
   :alt: PyPI version

.. image:: https://github.com/PyYoshi/cChardet/workflows/Build%20for%20Linux/badge.svg?branch=master
   :target: https://github.com/PyYoshi/cChardet/actions?query=workflow%3A%22Build+for+Linux%22
   :alt: Build for Linux

.. image:: https://github.com/PyYoshi/cChardet/workflows/Build%20for%20macOS/badge.svg?branch=master
   :target: https://github.com/PyYoshi/cChardet/actions?query=workflow%3A%22Build+for+macOS%22
   :alt: Build for macOS

.. image:: https://github.com/PyYoshi/cChardet/workflows/Build%20for%20windows/badge.svg?branch=master
   :target: https://github.com/PyYoshi/cChardet/actions?query=workflow%3A%22Build+for+windows%22
   :alt: Build for Windows

Supported Languages/Encodings
-----------------------------

-  International (Unicode)

   -  UTF-8
   -  UTF-16BE / UTF-16LE
   -  UTF-32BE / UTF-32LE / X-ISO-10646-UCS-4-34121 /
      X-ISO-10646-UCS-4-21431

-  Arabic

   -  ISO-8859-6
   -  WINDOWS-1256

-  Bulgarian

   -  ISO-8859-5
   -  WINDOWS-1251

-  Chinese

   -  ISO-2022-CN
   -  BIG5
   -  EUC-TW
   -  GB18030
   -  HZ-GB-2312

-  Croatian:

   -  ISO-8859-2
   -  ISO-8859-13
   -  ISO-8859-16
   -  Windows-1250
   -  IBM852
   -  MAC-CENTRALEUROPE

-  Czech

   -  Windows-1250
   -  ISO-8859-2
   -  IBM852
   -  MAC-CENTRALEUROPE

-  Danish

   -  ISO-8859-1
   -  ISO-8859-15
   -  WINDOWS-1252

-  English

   -  ASCII

-  Esperanto

   -  ISO-8859-3

-  Estonian

   -  ISO-8859-4
   -  ISO-8859-13
   -  ISO-8859-13
   -  Windows-1252
   -  Windows-1257

-  Finnish

   -  ISO-8859-1
   -  ISO-8859-4
   -  ISO-8859-9
   -  ISO-8859-13
   -  ISO-8859-15
   -  WINDOWS-1252

-  French

   -  ISO-8859-1
   -  ISO-8859-15
   -  WINDOWS-1252

-  German

   -  ISO-8859-1
   -  WINDOWS-1252

-  Greek

   -  ISO-8859-7
   -  WINDOWS-1253

-  Hebrew

   -  ISO-8859-8
   -  WINDOWS-1255

-  Hungarian:

   -  ISO-8859-2
   -  WINDOWS-1250

-  Irish Gaelic

   -  ISO-8859-1
   -  ISO-8859-9
   -  ISO-8859-15
   -  WINDOWS-1252

-  Italian

   -  ISO-8859-1
   -  ISO-8859-3
   -  ISO-8859-9
   -  ISO-8859-15
   -  WINDOWS-1252

-  Japanese

   -  ISO-2022-JP
   -  SHIFT\_JIS
   -  EUC-JP

-  Korean

   -  ISO-2022-KR
   -  EUC-KR / UHC

-  Lithuanian

   -  ISO-8859-4
   -  ISO-8859-10
   -  ISO-8859-13

-  Latvian

   -  ISO-8859-4
   -  ISO-8859-10
   -  ISO-8859-13

-  Maltese

   -  ISO-8859-3

-  Polish:

   -  ISO-8859-2
   -  ISO-8859-13
   -  ISO-8859-16
   -  Windows-1250
   -  IBM852
   -  MAC-CENTRALEUROPE

-  Portuguese

   -  ISO-8859-1
   -  ISO-8859-9
   -  ISO-8859-15
   -  WINDOWS-1252

-  Romanian:

   -  ISO-8859-2
   -  ISO-8859-16
   -  Windows-1250
   -  IBM852

-  Russian

   -  ISO-8859-5
   -  KOI8-R
   -  WINDOWS-1251
   -  MAC-CYRILLIC
   -  IBM866
   -  IBM855

-  Slovak

   -  Windows-1250
   -  ISO-8859-2
   -  IBM852
   -  MAC-CENTRALEUROPE

-  Slovene

   -  ISO-8859-2
   -  ISO-8859-16
   -  Windows-1250
   -  IBM852
   -  M

Example
-------

.. code-block:: python

    # -*- coding: utf-8 -*-
    import cchardet as chardet
    with open(r"src/tests/samples/wikipediaJa_One_Thousand_and_One_Nights_SJIS.txt", "rb") as f:
        msg = f.read()
        result = chardet.detect(msg)
        print(result)

Benchmark
---------

.. code-block:: bash

    $ cd src/
    $ pip install chardet
    $ python tests/bench.py


Results
~~~~~~~

CPU: Intel(R) Core(TM) i5-4690 CPU @ 3.50GHz

RAM: DDR3 1600Mhz 16GB

Platform: Ubuntu 16.04 amd64

Python 3.6.1
^^^^^^^^^^^^

+-----------------+------------------+
|                 | Request (call/s) |
+=================+==================+
| chardet v3.0.2  |       0.35       |
+-----------------+------------------+
| cchardet v2.0.1 |     1467.77      |
+-----------------+------------------+


LICENSE
-------

See **COPYING** file.

Contact
-------

- `Issues`_


.. _uchardet: https://github.com/PyYoshi/uchardet
.. _Issues: https://github.com/PyYoshi/cChardet/issues?page=1&state=open

Platform
--------

Support
~~~~~~~

- Windows i686, x86_64
- Linux i686, x86_64
- macOS x86_64

Do not Support
~~~~~~~~~~~~~~

- `Anaconda`_
- `pyenv`_

.. _Anaconda: https://www.anaconda.com/
.. _pyenv: https://github.com/pyenv/pyenv
