
.. include:: ../defs.rst

:mod:`i2c` - |I2C| API
----------------------

.. module :: pyftdi.i2c


Quickstart
~~~~~~~~~~

Example: communication with an |I2C| GPIO expander

.. code-block:: python

    # Instantiate an I2C controller
    i2c = I2cController()

    # Configure the first interface (IF/1) of the FTDI device as an I2C master
    i2c.configure('ftdi://ftdi:2232h/1')

    # Get a port to an I2C slave device
    slave = i2c.get_port(0x21)

    # Send one byte, then receive one byte
    slave.exchange([0x04], 1)

    # Write a register to the I2C slave
    slave.write_to(0x06, b'\x00')

    # Read a register from the I2C slave
    slave.read_from(0x00, 1)

Example: mastering the |I2C| bus with a complex transaction

.. code-block:: python

   from time import sleep

   port = I2cController().get_port(0x56)

   # emit a START sequence is read address, but read no data and keep the bus
   # busy
   port.read(0, relax=False)

   # wait for ~1ms
   sleep(0.001)

   # write 4 bytes, without neither emitting the start or stop sequence
   port.write(b'\x00\x01', relax=False, start=False)

   # read 4 bytes, without emitting the start sequence, and release the bus
   port.read(4, start=False)

See also pyi2cflash_ module and ``tests/i2c.py``, which provide more detailed
examples on how to use the |I2C| API.


Classes
~~~~~~~

.. autoclass :: I2cPort
 :members:

.. autoclass :: I2cGpioPort
 :members:

.. autoclass :: I2cController
 :members:


Exceptions
~~~~~~~~~~

.. autoexception :: I2cIOError
.. autoexception :: I2cNackError
.. autoexception:: I2cTimeoutError


GPIOs
~~~~~

See :doc:`../gpio` for details

Tests
~~~~~

|I2C| sample tests expect:
  * TCA9555 device on slave address 0x21
  * ADXL345 device on slave address 0x53

Checkout a fresh copy from PyFtdi_ github repository.

See :doc:`../pinout` for FTDI wiring.

.. code-block:: shell

   # optional: specify an alternative FTDI device
   export FTDI_DEVICE=ftdi://ftdi:2232h/1
   # optional: increase log level
   export FTDI_LOGLEVEL=DEBUG
   # be sure to connect the appropriate I2C slaves to the FTDI I2C bus and run
   PYTHONPATH=. python3 pyftdi/tests/i2c.py


.. _i2c_limitations:

Caveats
~~~~~~~

Open-collector bus
``````````````````

|I2C| uses only two bidirectional open collector (or open drain) lines, pulled
up with resistors. These resistors are also required on an |I2C| bus when an
FTDI master is used.

However, most FTDI devices do not use open collector outputs. Some software
tricks are used to fake open collector mode when possible, for example to
sample for slave ACK/NACK, but most communication (R/W, addressing, data)
cannot use open collector mode. This means that most FTDI devices source
current to the SCL and SDA lines. FTDI HW is able to cope with conflicting
signalling, where FTDI HW forces a line the high logical level while a slave
forces it to the low logical level, and limits the sourced current. You may
want to check your schematics if the slave is not able to handle 4 .. 16 mA
input current in SCL and SDA, for example. The maximal source current depends
on the FTDI device and the attached EEPROM configuration which may be used to
limit further down the sourced current.

Fortunately, FT232H device is fitted with real open collector outputs, and
PyFtdi always enable this mode on SCL and SDA lines when a FT232H device is
used.

Other FTDI devices such as FT2232H, FT4232H and FT4232HA do not support open
collector mode, and source current to SCL and SDA lines.

Clock streching
```````````````

Clock stretching is supported through a hack that re-uses the JTAG adaptative
clock mode designed for ARM devices. FTDI HW drives SCL on ``AD0`` (`BD0`), and
samples the SCL line on : the 8\ :sup:`th` pin of a port ``AD7`` (``BD7``).

When a FTDI device without an open collector capability is used
(FT2232H, FT4232H, FT4232HA) the current sourced from AD0 may prevent proper
sampling ofthe SCL line when the slave attempts to strech the clock. It is
therefore recommended to add a low forward voltage drop diode to `AD0` to
prevent AD0 to source current to the SCL bus. See the wiring section.

Speed
`````

Due to the FTDI MPSSE engine limitations, the actual bitrate for write
operations over I2C is very slow. As the I2C protocol enforces that each I2C
exchanged byte needs to be acknowledged by the peer, a I2C byte cannot be
written to the slave before the previous byte has been acknowledged by the
slave and read back by the I2C master, that is the host. This requires several
USB transfer for each byte, on top of each latency of the USB stack may add up.
With the introduction of PyFtdi_ v0.51, read operations have been optimized so
that long read operations are now much faster thanwith previous PyFtdi_
versions, and exhibits far shorter latencies.

Use of PyFtdi_ should nevetherless carefully studied and is not recommended if
you need to achieve medium to high speed write operations with a slave
(relative to the I2C clock...). Dedicated I2C master such as FT4222H device is
likely a better option, but is not currently supported with PyFtdi_ as it uses
a different communication protocol.

.. _i2c_wiring:

Wiring
~~~~~~

.. figure:: ../images/i2c_wiring.png
   :scale: 50 %
   :alt: I2C wiring
   :align: right

   Fig.1: FT2232H with clock stretching

* ``AD0`` should be connected to the SCL bus
* ``AD1`` and ``AD2`` should be both connected to the SDA bus
* ``AD7`` should be connected to the SCL bus, if clock streching is required
* remaining pins can be freely used as regular GPIOs.

*Fig.1*:

* ``D1`` is only required when clock streching is used along with
  FT2232H, FT4232H or FT4232HA devices. It should not be fit with an FT232H.
* ``AD7`` may be used as a regular GPIO with clock stretching is not required.
