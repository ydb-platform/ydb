.. -*- coding: utf-8 -*-

.. include:: ../defs.rst

:mod:`gpio` - GPIO API
----------------------

.. module :: pyftdi.gpio

Direct drive GPIO pins of FTDI device.

.. note::

  This mode is mutually exclusive with advanced serial MPSSE features, such as
  |I2C|, SPI, JTAG, ...

  If you need to use GPIO pins and MPSSE interface on the same port, you need
  to use the dedicated API. This shared mode is supported with the
  :doc:`SPI API <spi>` and the :doc:`I2C API <i2c>`.

.. warning::

  This API does not provide access to the special CBUS port of FT232R, FT232H,
  FT230X and FT231X devices. See :ref:`cbus_gpio` for details.

Quickstart
~~~~~~~~~~

See ``tests/gpio.py`` example


Classes
~~~~~~~

.. autoclass :: GpioPort

.. autoclass :: GpioAsyncController
 :members:

.. autoclass :: GpioSyncController
 :members:

.. autoclass :: GpioMpsseController
 :members:


Exceptions
~~~~~~~~~~

.. autoexception :: GpioException


Info about GPIO API
~~~~~~~~~~~~~~~~~~~

See :doc:`../gpio` for details
