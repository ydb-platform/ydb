.. include:: ../defs.rst

:mod:`serialext` - UART API
---------------------------

There is no dedicated module for the UART API, as PyFtdi_ acts as a backend of
the well-known pyserial_ module.

The pyserial_ backend module is implemented as the `serialext.protocol_ftdi`
module. It is not documented here as no direct call to this module is required,
as the UART client should use the regular pyserial_ API.

Usage
~~~~~

To enable PyFtdi_ as a pyserial_ backend, use the following import:

.. code-block:: python

    import pyftdi.serialext

Then use

.. code-block:: python

    pyftdi.serialext.serial_for_url(url, **options)

to open a pyserial_ serial port instance.


Quickstart
~~~~~~~~~~

.. code-block:: python

    # Enable pyserial extensions
    import pyftdi.serialext

    # Open a serial port on the second FTDI device interface (IF/2) @ 3Mbaud
    port = pyftdi.serialext.serial_for_url('ftdi://ftdi:2232h/2', baudrate=3000000)

    # Send bytes
    port.write(b'Hello World')

    # Receive bytes
    data = port.read(1024)

.. _uart_gpio:

GPIO access
~~~~~~~~~~~

UART mode, the primary function of FTDI \*232\* devices, is somewhat limited
when it comes to GPIO management, as opposed to alternative mode such as |I2C|,
SPI and JTAG. It is not possible to assign the unused pins of an UART mode to
arbitrary GPIO functions.

All the 8 lower pins of an UART port are dedicated to the UART function,
although most of them are seldomely used, as dedicated to manage a modem or a
legacy DCE_ device. Upper pins (b\ :sub:`7`\ ..b\ :sub:`15`\ ), on devices that
have ones, cannot be driven while UART port is enabled.

It is nevertheless possible to have limited access to the lower pins as GPIO,
with many limitations:

- the GPIO direction of each pin is hardcoded and cannot be changed
- GPIO pins cannot be addressed atomically: it is possible to read the state
  of an input GPIO, or to change the state of an output GPIO, one after
  another. This means than obtaining the state of several input GPIOs or
  changing the state of several output GPIO at once is not possible.
- some pins cannot be used as GPIO is hardware flow control is enabled.
  Keep in mind However that HW flow control with FTDI is not reliable, see the
  :ref:`hardware_flow_control` section.

Accessing those GPIO pins is done through the UART extended pins, using their
UART assigned name, as PySerial port attributes. See the table below:

+---------------+------+-----------+-------------------------------+
| Bit           | UART | Direction | API                           |
+===============+======+===========+===============================+
| b\ :sub:`0`\  | TX   | Out       | ``port.write(buffer)``        |
+---------------+------+-----------+-------------------------------+
| b\ :sub:`1`\  | RX   | In        | ``buffer = port.read(count)`` |
+---------------+------+-----------+-------------------------------+
| b\ :sub:`2`\  | RTS  | Out       | ``port.rts = state``          |
+---------------+------+-----------+-------------------------------+
| b\ :sub:`3`\  | CTS  | In        | ``state = port.cts``          |
+---------------+------+-----------+-------------------------------+
| b\ :sub:`4`\  | DTR  | Out       | ``port.dtr = state``          |
+---------------+------+-----------+-------------------------------+
| b\ :sub:`5`\  | DSR  | In        | ``state = port.dsr``          |
+---------------+------+-----------+-------------------------------+
| b\ :sub:`6`\  | DCD  | In        | ``state = port.dcd``          |
+---------------+------+-----------+-------------------------------+
| b\ :sub:`7`\  | RI   | In        | ``state = port.ri``           |
+---------------+------+-----------+-------------------------------+

CBUS support
````````````

Some FTDI devices (FT232R, FT232H, FT230X, FT231X) support additional CBUS
pins, which can be used as regular GPIOs pins. See :ref:`CBUS GPIO<cbus_gpio>`
for details.


.. _pyterm:

Mini serial terminal
~~~~~~~~~~~~~~~~~~~~

``pyterm.py`` is a simple serial terminal that can be used to test the serial
port feature. See the :ref:`tools` chapter to locate this tool.

::

  Usage: pyterm.py [-h] [-f] [-b BAUDRATE] [-w] [-e] [-r] [-l] [-s] [-P VIDPID]
                   [-V VIRTUAL] [-v] [-d]
                   [device]

  Simple Python serial terminal

  positional arguments:
    device                serial port device name (default: ftdi:///1)

  optional arguments:
    -h, --help            show this help message and exit
    -f, --fullmode        use full terminal mode, exit with [Ctrl]+B
    -b BAUDRATE, --baudrate BAUDRATE
                          serial port baudrate (default: 115200)
    -w, --hwflow          hardware flow control
    -e, --localecho       local echo mode (print all typed chars)
    -r, --crlf            prefix LF with CR char, use twice to replace all LF
                          with CR chars
    -l, --loopback        loopback mode (send back all received chars)
    -s, --silent          silent mode
    -P VIDPID, --vidpid VIDPID
                          specify a custom VID:PID device ID, may be repeated
    -V VIRTUAL, --virtual VIRTUAL
                          use a virtual device, specified as YaML
    -v, --verbose         increase verbosity
    -d, --debug           enable debug mode

If the PyFtdi module is not yet installed and ``pyterm.py`` is run from the
archive directory, ``PYTHONPATH`` should be defined to the current directory::

    PYTHONPATH=$PWD pyftdi/bin/pyterm.py ftdi:///?

The above command lists all the available FTDI device ports. To avoid conflicts
with some shells such as `zsh`, escape the `?` char as ``ftdi:///\?``.

To start up a serial terminal session, specify the FTDI port to use, for
example:

.. code-block:: shell

    # detect all FTDI connected devices
    PYTHONPATH=. python3 pyftdi/bin/ftdi_urls.py
    # use the first interface of the first FT2232H as a serial port
    PYTHONPATH=$PWD pyftdi/bin/pyterm.py ftdi://ftdi:2232/1


.. _uart-limitations:

Limitations
~~~~~~~~~~~

Although the FTDI H series are in theory capable of 12 MBps baudrate, baudrates
above 6 Mbps are barely usable.

See the following table for details.

+------------+-------------+------------+-------------+------------+--------+
|  Requ. bps |HW capability| 9-bit time |  Real bps   | Duty cycle | Stable |
+============+=============+============+=============+============+========+
| 115.2 Kbps |  115.2 Kbps |   78.08 µs | 115.26 Kbps |     49.9%  |  Yes   |
+------------+-------------+------------+-------------+------------+--------+
| 460.8 Kbps | 461.54 Kbps |   19.49 µs | 461.77 Kbps |     49.9%  |  Yes   |
+------------+-------------+------------+-------------+------------+--------+
|     1 Mbps |      1 Mbps |   8.98 µs  |  1.002 Mbps |     49.5%  |  Yes   |
+------------+-------------+------------+-------------+------------+--------+
|     4 Mbps |      4 Mbps |   2.24 µs  |  4.018 Mbps |       48%  |  Yes   |
+------------+-------------+------------+-------------+------------+--------+
|     5 Mbps |  5.052 Mbps |   1.78 µs  |  5.056 Mbps |       50%  |  Yes   |
+------------+-------------+------------+-------------+------------+--------+
|     6 Mbps |      6 Mbps |   1.49 µs  |  6.040 Mbps |     48.5%  |  Yes   |
+------------+-------------+------------+-------------+------------+--------+
|     7 Mbps |  6.857 Mbps |   1.11 µs  |  8.108 Mbps |       44%  |   No   |
+------------+-------------+------------+-------------+------------+--------+
|     8 Mbps |      8 Mbps |   1.11 µs  |  8.108 Mbps |   44%-48%  |   No   |
+------------+-------------+------------+-------------+------------+--------+
|   8.8 Mbps |  8.727 Mbps |   1.13 µs  |  7.964 Mbps |       44%  |   No   |
+------------+-------------+------------+-------------+------------+--------+
|   9.6 Mbps |    9.6 Mbps |   1.12 µs  |  8.036 Mbps |       48%  |   No   |
+------------+-------------+------------+-------------+------------+--------+
|  10.5 Mbps | 10.667 Mbps |   1.11 µs  |  8.108 Mbps |       44%  |   No   |
+------------+-------------+------------+-------------+------------+--------+
|    12 Mbps |     12 Mbps |   0.75 µs  |     12 Mbps |       43%  |  Yes   |
+------------+-------------+------------+-------------+------------+--------+

 * 9-bit time is the measured time @ FTDI output pins for a 8-bit character
   (start bit + 8 bit data)
 * Duty cycle is the ratio between a low-bit duration and a high-bit duration,
   a good UART should exhibit the same duration for low bits and high bits,
   *i.e.* a duty cycle close to 50%.
 * Stability reports whether subsequent runs, with the very same HW settings,
   produce the same timings.

Achieving a reliable connection over 6 Mbps has proven difficult, if not
impossible: Any baudrate greater than 6 Mbps (except the upper 12 Mbps limit)
results into an actual baudrate of about 8 Mbps, and suffer from clock
fluterring [7.95 .. 8.1Mbps].

.. _hardware_flow_control:

Hardware flow control
`````````````````````

Moreover, as the hardware flow control of the FTDI device is not a true HW
flow control. Quoting FTDI application note:

   *If CTS# is logic 1 it is indicating the external device cannot accept more
   data. the FTxxx will stop transmitting within 0~3 characters, depending on
   what is in the buffer.*
   **This potential 3 character overrun does occasionally present problems.**
   *Customers shoud be made aware the FTxxx is a USB device and not a "normal"
   RS232 device as seen on a PC. As such the device operates on a packet
   basis as opposed to a byte basis.*

