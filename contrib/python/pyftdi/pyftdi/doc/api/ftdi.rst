.. -*- coding: utf-8 -*-

.. include:: ../defs.rst

:mod:`ftdi` - FTDI low-level driver
-----------------------------------

.. module :: pyftdi.ftdi

This module implements access to the low level FTDI hardware. There are very
few reasons to use this module directly. Most of PyFtdi_ features are available
through the dedicated :doc:`APIs <index>`.

Classes
~~~~~~~

.. autoclass :: Ftdi
 :members:


Exceptions
~~~~~~~~~~

.. autoexception :: FtdiError
.. autoexception :: FtdiMpsseError
.. autoexception :: FtdiFeatureError
