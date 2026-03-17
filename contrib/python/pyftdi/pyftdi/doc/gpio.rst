.. include:: defs.rst

GPIOs
-----

Overview
~~~~~~~~

Many PyFtdi APIs give direct access to the IO pins of the FTDI devices:

  * *GpioController*, implemented as ``GpioAsyncController``,
    ``GpioSyncController`` and ``GpioMpsseController`` (see :doc:`api/gpio`)
    gives full access to the FTDI pins as raw I/O pins,
  * ``SpiGpioPort`` (see :doc:`api/spi`) gives access to all free pins of an
    FTDI interface, which are not reserved for the SPI feature,
  * ``I2cGpioPort`` (see :doc:`api/i2c`) gives access to all free pins of an
    FTDI interface, which are not reserved for the I2C feature

Other modes
```````````

  * Gpio raw access is not yet supported with JTAG feature.
  * It is not possible to use GPIO along with UART mode on the same interface.
    However, UART mode still provides (very) limited access to GPIO pins, see
    UART :ref:`uart_gpio` for details.

This document presents the common definitions for these APIs and explain how to
drive those pins.


Definitions
~~~~~~~~~~~

Interfaces
``````````

An FTDI *interface* follows the definition of a *USB interface*: it is an
independent hardware communication port with an FTDI device. Each interface can
be configured independently from the other interfaces on the same device, e.g.
one interface may be configured as an UART, the other one as |I2C| + GPIO.

It is possible to access two distinct interfaces of the same FTDI device
from a multithreaded application, and even from different applications, or
Python interpreters. However two applications cannot access the same interface
at the same time.

.. warning::

   Performing a USB device reset affects all the interfaces of an FTDI device,
   this is the rationale for not automatically performing a device reset when
   an interface is initialiazed and configured from PyFtdi_.

.. _ftdi_ports:

Ports
`````

An FTDI port is ofter used in PyFtdi as a synonym for an interface. This may
differ from the FTDI datasheets that sometimes show an interface with several
ports (A\*BUS, B\*BUS). From a software standpoint, ports and interfaces are
equivalent: APIs access all the HW port from the same interface at once. From a
pure hardware standpoint, a single interface may be depicted as one or two
*ports*.

With PyFtdi_, *ports* and *interfaces* should be considered as synomyms.

Each port can be accessed as raw input/output pins. At a given time, a pin is
either configured as an input or an output function.

The width of a port, that is the number of pins of the interface, depending on
the actual hardware, *i.e.* the FTDI model:

* FT232R features a single port, which is 8-bit wide: `DBUS`,
* FT232H features a single port, which is 16-bit wide: `ADBUS/ACBUS`,
* FT2232D features two ports, which are 12-bit wide each: `ADBUS/ACBUS` and
  `BDBUS/BCBUS`,
* FT2232H features two ports, which are 16-bit wide each: `ADBUS/ACBUS` and
  `BDBUS/BCBUS`,
* FT4232H/FT4232HA features four ports, which are 8-bit wide each: `ADBUS`,
  `BDBUS`, `CDBUS` and `DDBUS`,
* FT230X features a single port, which is 4-bit wide,
* FT231X feature a single port, which is 8-bit wide

For historical reasons, 16-bit ports used to be named *wide* ports and 8-bit
ports used to be called *narrow* with PyFtdi_. This terminology and APIs are
no longer used, but are kept to prevent API break. Please only use the port
``width`` rather than these legacy port types.


GPIO value
``````````

* A logical ``0`` bit represents a low level value on a pin, that is *GND*
* A logical ``1`` bit represents a high level value on a pin, that is *Vdd*
  which is typically 3.3 volts on most FTDIs

Please refers to the FTDI datasheet of your device for the tolerance and
supported analog levels for more details

.. hint::

   FT232H supports a specific feature, which is dedicated to better supporting
   the |I2C| feature. This specific devices enables an open-collector mode:

   * Setting a pin to a low level drains it to *GND*
   * Setting a pin to a high level sets the pin as High-Z

   This feature is automatically activated when |I2C| feature is enabled on a
   port, for the two first pins, i.e. `SCL` and `SDA out`.

   However, PyFTDI does not yet provide an API to enable this mode to the
   other pins of a port, *i.e.* for the pins used as GPIOs.


Direction
`````````

An FTDI pin should either be configured as an input or an ouput. It is
mandatory to (re)configure the direction of a pin before changing the way it is
used.

* A logical ``0`` bit represents an input pin, *i.e.* a pin whose value can be
  sampled and read via the PyFTDI APIs
* A logical ``1`` bit represents an output pin, *i.e.* a pin whose value can be
  set/written with the PyFTDI APIs


.. _cbus_gpio:

CBUS GPIOs
~~~~~~~~~~

FT232R, FT232H and FT230X/FT231X support an additional port denoted CBUS:

* FT232R provides an additional 5-bit wide port, where only 4 LSBs can be
  used as programmable GPIOs: ``CBUS0`` to ``CBUS3``,
* FT232H provices an additional 10-bit wide port, where only 4 pins can be
  used as programmable GPIOs: ``CBUS5``, ``CBUS6``, ``CBUS8``, ``CBUS9``
* FT230X/FT231X provides an additional 4-bit wide port: ``CBUS0`` to ``CBUS3``

Note that CBUS access is slower than regular asynchronous bitbang mode.

CBUS EEPROM configuration
`````````````````````````

Accessing this extra port requires a specific EEPROM configuration.

The EEPROM needs to be configured so that the CBUS pins that need to be used
as GPIOs are defined as ``GPIO``. Without this special configuration, CBUS
pins are used for other functions, such as driving leds when data is exchanged
over the UART port. Remember to power-cycle the FTDI device after changing its
EEPROM configuration to force load the new configuration.

The :ref:`ftconf` tool can be used to query and change the EEPROM
configuration. See the EEPROM configuration :ref:`example <eeprom_cbus>`.

CBUS GPIO API
`````````````

PyFtdi_ starting from v0.47 supports CBUS pins as special GPIO port. This port
is *not* mapped as regular GPIO, a dedicated API is reserved to drive those
pins:

* :py:meth:`pyftdi.ftdi.Ftdi.has_cbus` to report whether the device supports
  CBUS gpios,
* :py:meth:`pyftdi.ftdi.Ftdi.set_cbus_direction` to configure the port,
* :py:meth:`pyftdi.ftdi.Ftdi.get_cbus_gpio` to get the logical values from the
  port,
* :py:meth:`pyftdi.ftdi.Ftdi.set_cbus_gpio` to set new logical values to the
  port

Additionally, the EEPROM configuration can be queried to retrieve which CBUS
pins have been assigned to GPIO functions:

* :py:meth:`pyftdi.eeprom.FtdiEeprom.cbus_pins` to report CBUS GPIO pins

The CBUS port is **not** available through the
:py:class:`pyftdi.gpio.GpioController` API, as it cannot be considered as a
regular GPIO port.

.. warning::

  CBUS GPIO feature has only be tested with the virtual test framework and a
  real FT231X HW device. It should be considered as an experimental feature
  for now.

Configuration
~~~~~~~~~~~~~

GPIO bitmap
```````````

The GPIO pins of a port are always accessed as an integer, whose supported
width depends on the width of the port. These integers should be considered as
a bitmap of pins, and are always assigned the same mapping, whatever feature is
enabled:

* b\ :sub:`0`\  (``0x01``) represents the first pin of a port, *i.e.* AD0/BD0
* b\ :sub:`1`\  (``0x02``) represents the second pin of a port, *i.e.* AD1/BD1
* ...
* b\ :sub:`7`\  (``0x80``) represents the eighth pin of a port, *i.e.* AD7/BD7
* b\ :sub:`N`\  represents the highest pin of a port, *i.e.* AD7/BD7 for an
  8-bit port, AD15/BD15 for a 16-bit port, etc.

Pins reserved for a specific feature (|I2C|, SPI, ...) cannot be accessed as
a regular GPIO. They cannot be arbitrarily written and should be masked out
when the GPIO output value is set. See :ref:`reserved_pins` for details.

FT232H CBUS exception
.....................

Note that there is an exception to this rule for FT232H CBUS port: FTDI has
decided to map non-contiguous CBUS pins as GPIO-capable CBUS pins, that is
``CBUS5``, ``CBUS6``, ``CBUS8``, ``CBUS9``, where other CBUS-enabled devices
use ``CBUS0``, ``CBUS1``, ``CBUS2``, ``CBUS3``.

If the CBUS GPIO feature is used with an FT232H device, the pin positions for
the GPIO port are not b\ :sub:`5`\  .. b\ :sub:`9`\  but b\ :sub:`0`\  to
b\ :sub:`3`\  . This may sounds weird, but CBUS feature is somewhat hack-ish
even with FTDI commands, so it did not deserve a special treatment for the sake
of handling the weird implementation of FT232H.

Direction bitmap
````````````````

Before using a port as GPIO, the port must be configured as GPIO. This is
achieved by either instanciating one of the *GpioController* or by requesting
the GPIO port from a specific serial bus controller:
``I2cController.get_gpio()`` and ``SpiController.get_gpio()``. All instances
provide a similar API (duck typing API) to configure, read and write to GPIO
pins.

Once a GPIO port is instanciated, the direction of each pin should be defined.
The direction can be changed at any time. It is not possible to write to /
read from a pin before the proper direction has been defined.

To configure the direction, use the `set_direction` API with a bitmap integer
value that defines the direction to use of each pin.

Direction example
.................

A 8-bit port, dedicated to GPIO, is configured as follows:

 * BD0, BD3, BD7: input, `I` for short
 * BD1-BD2, BD4-BD6: output, `O` for short

That is, MSB to LSB: *I O O O I O O I*.

This translates to 0b ``0111 0110`` as output is ``1`` and input is ``0``,
that is ``0x76`` as an hexa value. This is the direction value to use to
``configure()`` the port.

See also the ``set_direction()`` API to reconfigure the direction of GPIO pins
at any time. This method accepts two arguments. This first arguments,
``pins``, defines which pins - the ones with the maching bit set - to consider
in the second ``direction`` argument, so there is no need to
preserve/read-modify-copy the configuration of other pins. Pins with their
matching bit reset are not reconfigured, whatever their direction bit.

.. code-block:: python

    gpio = GpioAsyncController()
    gpio.configure('ftdi:///1', direction=0x76)
    # later, reconfigure BD2 as input and BD7 as output
    gpio.set_direction(0x84, 0x80)


Using GPIO APIs
~~~~~~~~~~~~~~~

There are 3 variant of *GpioController*, depending on which features are needed
and how the GPIO port usage is intended. :doc:`api/gpio` gives in depth details
about those controllers. Those controllers are mapped onto FTDI HW features.

* ``GpioAsyncController`` is likely the most useful API to drive GPIOs.

  It enables reading current GPIO input pin levels and to change GPIO output
  pin levels. When vector values (byte buffers) are used instead of scalar
  value (single byte), GPIO pins are samples/updated at a regular pace, whose
  frequency can be configured. It is however impossible to control the exact
  time when input pins start to be sampled, which can be tricky to use with
  most applications. See :doc:`api/gpio` for details.

* ``GpioSyncController`` is a variant of the previous API.

  It is aimed at precise time control of sampling/updating the GPIO: a new
  GPIO input sample is captured once every time GPIO output pins are updated.
  With byte buffers, GPIO pins are samples/updated at a regular pace, whose
  frequency can be configured as well. The API of ``GpioSyncController``
  slightly differ from the other GPIO APIs, as the usual ``read``/``write``
  method are replaced with a single ``exchange`` method.

Both ``GpioAsyncController`` and ``GpioSyncController`` are restricted to only
access the 8 LSB pins of a port, which means that FTDI device with wider port
(12- and 16- pins) cannot be fully addressed, as only b\ :sub:`0`\  to b\
:sub:`7`\  can be addressed.

* ``GpioMpsseController`` enables access to the MSB pins of wide ports.

  However LSB and MSB pins cannot be addressed in a true atomic manner, which
  means that there is a short delay between sampling/updating the LSB and MSB
  part of the same wide port. Byte buffer can also be sampled/updated at a
  regular pace, but the achievable frequency range may differ from the other
  controllers.

It is recommened to read the ``tests/gpio.py`` files - available from GitHub -
to get some examples on how to use these API variants.

Setting GPIO pin state
``````````````````````

To write to a GPIO, use the `write()` method. The caller needs to mask out
the bits configured as input, or an exception is triggered:

* writing ``0`` to an input pin is ignored
* writing ``1`` to an input pin raises an exception

.. code-block:: python

    gpio = GpioAsyncController()
    gpio.configure('ftdi:///1', direction=0x76)
    # all output set low
    gpio.write(0x00)
    # all output set high
    gpio.write(0x76)
    # all output set high, apply direction mask
    gpio.write(0xFF & gpio.direction)
    # all output forced to high, writing to input pins is illegal
    gpio.write(0xFF)  # raises an IOError
    gpio.close()


Retrieving GPIO pin state
`````````````````````````

To read a GPIO, use the `read()` method.

.. code-block:: python

    gpio = GpioAsyncController()
    gpio.configure('ftdi:///1', direction=0x76)
    # read whole port
    pins = gpio.read()
    # ignore output values (optional)
    pins &= ~gpio.direction
    gpio.close()


Modifying GPIO pin state
````````````````````````

A read-modify-write sequence is required.

.. code-block:: python

    gpio = GpioAsyncController()
    gpio.configure('ftdi:///1', direction=0x76)
    # read whole port
    pins = gpio.read()
    # clearing out AD1 and AD2
    pins &= ~((1 << 1) | (1 << 2))  # or 0x06
    # want AD2=0, AD1=1
    pins |= 1 << 1
    # update GPIO output
    gpio.write(pins)
    gpio.close()


Synchronous GPIO access
```````````````````````

.. code-block:: python

    gpio = GpioSyncController()
    gpio.configure('ftdi:///1', direction=0x0F, frequency=1e6)
    outs = bytes(range(16))
    ins = gpio.exchange(outs)
    # ins contains as many bytes as outs
    gpio.close()


CBUS GPIO access
````````````````

.. code-block:: python

   ftdi = Ftdi()
   ftdi.open_from_url('ftdi:///1')
   # validate CBUS feature with the current device
   assert ftdi.has_cbus
   # validate CBUS EEPROM configuration with the current device
   eeprom = FtdiEeprom()
   eeprom.connect(ftdi)
   # here we use CBUS0 and CBUS3 (or CBUS5 and CBUS9 on FT232H)
   assert eeprom.cbus_mask & 0b1001 == 0b1001
   # configure CBUS0 as output and CBUS3 as input
   ftdi.set_cbus_direction(0b1001, 0b0001)
   # set CBUS0
   ftdi.set_cbus_gpio(0x1)
   # get CBUS3
   cbus3 = ftdi.get_cbus_gpio() >> 3


.. code-block:: python

   # it is possible to open the ftdi object from an existing serial connection:
   port = serial_for_url('ftdi:///1')
   ftdi = port.ftdi
   ftdi.has_cbus
   # etc...

.. _reserved_pins:

Reserved pins
~~~~~~~~~~~~~

GPIO pins vs. feature pins
``````````````````````````

It is important to note that the reserved pins do not change the pin
assignment, *i.e.* the lowest pins of a port may become unavailable as regular
GPIO when the feature is enabled:

Example
.......

|I2C| feature reserves
the three first pins, as *SCL*, *SDA output*, *SDA input* (w/o clock stretching
feature which also reserves another pin). This means that AD0, AD1 and AD2,
that is b\ :sub:`0`\ , b\ :sub:`1`\ , b\ :sub:`2`\  cannot be directly
accessed.

The first accessible GPIO pin in this case is no longer AD0 but AD3, which
means that b\ :sub:`3`\ becomes the lowest bit which can be read/written.

.. code-block:: python

    # use I2C feature
    i2c = I2cController()
    # configure the I2C feature, and predefines the direction of the GPIO pins
    i2c.configure('ftdi:///1', direction=0x78)
    gpio = i2c.get_gpio()
    # read whole port
    pins = gpio.read()
    # clearing out I2C bits (SCL, SDAo, SDAi)
    pins &= 0x07
    # set AD4
    pins |= 1 << 4
    # update GPIO output
    gpio.write(pins)
