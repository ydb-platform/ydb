.. include:: ../defs.rst

:mod:`spi` - SPI API
--------------------

.. module :: pyftdi.spi

Quickstart
~~~~~~~~~~

Example: communication with a SPI data flash (half-duplex example)

.. code-block:: python

    # Instantiate a SPI controller
    spi = SpiController()

    # Configure the first interface (IF/1) of the FTDI device as a SPI master
    spi.configure('ftdi://ftdi:2232h/1')

    # Get a port to a SPI slave w/ /CS on A*BUS3 and SPI mode 0 @ 12MHz
    slave = spi.get_port(cs=0, freq=12E6, mode=0)

    # Request the JEDEC ID from the SPI slave
    jedec_id = slave.exchange([0x9f], 3)


Example: communication with a remote SPI device using full-duplex mode

.. code-block:: python

    # Instantiate a SPI controller
    # We need want to use A*BUS4 for /CS, so at least 2 /CS lines should be
    # reserved for SPI, the remaining IO are available as GPIOs.
    spi = SpiController(cs_count=2)

    # Configure the first interface (IF/1) of the FTDI device as a SPI master
    spi.configure('ftdi://ftdi:2232h/1')

    # Get a port to a SPI slave w/ /CS on A*BUS4 and SPI mode 2 @ 10MHz
    slave = spi.get_port(cs=1, freq=10E6, mode=2)

    # Synchronous exchange with the remote SPI slave
    write_buf = b'\x01\x02\x03'
    read_buf = slave.exchange(write_buf, duplex=True)

Example: communication with a SPI device and an extra GPIO

.. code-block:: python

    # Instantiate a SPI controller
    spi = SpiController()

    # Configure the first interface (IF/1) of the first FTDI device as a
    # SPI master
    spi.configure('ftdi://::/1')

    # Get a SPI port to a SPI slave w/ /CS on A*BUS3 and SPI mode 0 @ 12MHz
    slave = spi.get_port(cs=0, freq=12E6, mode=0)

    # Get GPIO port to manage extra pins, use A*BUS4 as GPO, A*BUS5 as GPI
    gpio = spi.get_gpio()
    gpio.set_direction(pins=0b0011_0000, direction=0b0001_0000)

    # Assert GPO pin
    gpio.write(0x10)
    # Write to SPI slace
    slave.write(b'hello world!')
    # Release GPO pin
    gpio.write(0x00)
    # Test GPI pin
    pin = bool(gpio.read() & 0x20)


Example: managing non-byte aligned transfers

.. code-block:: python

    # Instantiate a SPI controller
    spi = SpiController()

    # Configure the first interface (IF/1) of the first FTDI device as a
    # SPI master
    spi.configure('ftdi://::/1')

    # Get a SPI port to a SPI slave w/ /CS on A*BUS3
    slave = spi.get_port(cs=0)

    # write 6 first bits of a byte buffer
    slave.write(b'\xff', droptail=2)

    # read only 13 bits from a slave (13 clock cycles)
    # only the 5 MSBs of the last byte are valid, 3 LSBs are force to zero
    slave.read(2, droptail=3)

See also pyspiflash_ module and ``tests/spi.py``, which provide more detailed
examples on how to use the SPI API.


Classes
~~~~~~~

.. autoclass :: SpiPort
 :members:

.. autoclass :: SpiGpioPort
 :members:

.. autoclass :: SpiController
 :members:

Exceptions
~~~~~~~~~~

.. autoexception :: SpiIOError


GPIOs
~~~~~

See :doc:`../gpio` for details

Tests
~~~~~

SPI sample tests expect:
  * MX25L1606E device on /CS 0, SPI mode 0
  * ADXL345 device on /CS 1, SPI mode 2
  * RFDA2125 device on /CS 2, SPI mode 0

Checkout a fresh copy from PyFtdi_ github repository.

See :doc:`../pinout` for FTDI wiring.

.. code-block:: shell

   # optional: specify an alternative FTDI device
   export FTDI_DEVICE=ftdi://ftdi:2232h/1
   # optional: increase log level
   export FTDI_LOGLEVEL=DEBUG
   # be sure to connect the appropriate SPI slaves to the FTDI SPI bus and run
   PYTHONPATH=. python3 pyftdi/tests/spi.py

.. _spi_limitations:

Limitations
~~~~~~~~~~~

SPI Modes 1 & 3
```````````````

FTDI hardware does not support cpha=1 (mode 1 and mode 3). As stated in
Application Node 114:

   "*It is recommended that designers review the SPI Slave
   data sheet to determine the SPI mode implementation. FTDI device can only
   support mode 0 and mode 2 due to the limitation of MPSSE engine.*".

Support for mode 1 and mode 3 is implemented with some workarounds, but
generated signals may not be reliable: YMMV. It is only available with -H
series (232H, 2232H, 4232H, 4232HA).

The 3-clock phase mode which has initially be designed to cope with |I2C|
signalling is used to delay the data lines from the clock signals. A direct
consequence of this workaround is that SCLK duty cycle is not longer 50% but
25% (mode 1) or 75% (mode 3). Again, support for mode 1 and mode 3 should be
considered as a kludge, you've been warned.

Time-sensitive usage
````````````````````

Due to the MPSSE engine limitation, it is not possible to achieve
time-controlled request sequence. In other words, if the SPI slave needs to
receive command sequences at precise instants - for example ADC or DAC
devices - PyFtdi_ use is not recommended. This limitation is likely to apply
to any library that relies on FTDI device. The USB bus latency and the lack
of timestamped commands always add jitter and delays, with no easy known
workaround.

.. _spi_wiring:

Wiring
~~~~~~

.. figure:: ../images/spi_wiring.png
   :scale: 50 %
   :alt: SPI wiring
   :align: right

   Fig.1: FT2232H with two SPI slaves

* ``AD0`` should be connected to SCLK
* ``AD1`` should be connected to MOSI
* ``AD2`` should be connected to MISO
* ``AD3`` should be connected to the first slave /CS.
* ``AD4`` should be connected to the second slave /CS, if any
* remaining pins can be freely used as regular GPIOs.

*Fig.1*:

* ``AD4`` may be used as a regular GPIO if a single SPI slave is used
* ``AD5`` may be used as another /CS signal for a third slave, in this case
  the first available GPIO is ``AD6``, etc.
