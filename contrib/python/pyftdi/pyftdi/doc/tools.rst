.. include:: defs.rst

.. _tools:

Tools
-----

Overview
~~~~~~~~

PyFtdi_ comes with a couple of scripts designed to help using PyFtdi_ APIs,
and can be useful to quick start working with PyFtdi_.

Scripts
~~~~~~~

.. _ftdi_urls:

``ftdi_urls``
`````````````

This tiny script ``ftdi_urls.py`` to list the available, *i.e.* detected,
FTDI devices connected to the host, and the URLs than can be used to open a
:py:class:`pyftdi.ftdi.Ftdi` instance with the
:py:class:`pyftdi.ftdi.Ftdi.open_from_url` family and ``configure`` methods.


``ftconf``
``````````

``ftconf.py`` is a companion script to help managing the content of
the FTDI EEPROM from the command line. See the :ref:`ftconf` documentation.


.. _i2cscan:

``i2cscan``
```````````

The ``i2cscan.py`` script helps to discover which I2C devices
are connected to the FTDI I2C bus.


.. _pyterm.py:

``pyterm``
``````````

``pyterm.py`` is a simple serial terminal that can be used to test the serial
port feature, see the :ref:`pyterm` documentation.


Where to find these tools?
~~~~~~~~~~~~~~~~~~~~~~~~~~

These scripts can be downloaded from PyFtdiTools_, and are also installed along
with the PyFtdi_ module on the local host.

The location of the scripts depends on how PyFtdi_ has been installed and the
type of hosts:

* on linux and macOS, there are located in the ``bin/`` directory, that is the
  directory where the Python interpreter is installed.

* on Windows, there are located in the ``Scripts/`` directory, which is a
  subdirectory of the directory where the Python interpreter is installed.


.. _common_option_switches:

Common options switches
~~~~~~~~~~~~~~~~~~~~~~~

PyFtdi_ tools share many common option switches:

.. _option_d:

``-d``
  Enable debug mode, which emits Python traceback on exceptions

.. _option_h:

``-h``
  Show quick help and exit

.. _option_P_:

``-P <vidpid>``
  Add custom vendor and product identifiers.

  PyFtdi_ only recognizes FTDI official USB vendor identifier (*0x403*) and
  the USB identifiers of their products.

  In order to use alternative VID/PID values, the PyFtdi_ tools accept the
  ``-P`` option to describe those products

  The ``vidpid`` argument should match the following format:

  ``[vendor_name=]<vendor_id>:[product_name=]<product_id>``

  * ``vendor_name`` and ``product_name`` are optional strings, they may be
    omitted as they only serve as human-readable aliases for the vendor and
    product names. See example below.
  * ``vendor_id`` and ``product_id`` are mandatory strings that should resolve
    into 16-bit integers (USB VID and PID values). Integer values are always
    interpreted as hexadecimal values, *e.g.* `-P 1234:6789` is parsed as
    `-P 0x1234:0x6789`.

  This option may be repeated as many times as required to add support for
  several custom devices.

  examples:

   * ``0x403:0x9999``, *vid:pid* short syntax, with no alias names;
     a matching FTDI :ref:`URL <url_scheme>` would be ``ftdi://ftdi:0x9999/1``
   * ``mycompany=0x666:myproduct=0xcafe``, *vid:pid* complete syntax with
     aliases; matching FTDI :ref:`URLs <url_scheme>` could be:

     * ``ftdi://0x666:0x9999/1``
     * ``ftdi://mycompany:myproduct/1``
     * ``ftdi://mycompany:0x9999/1``
     * ...

.. _option_v:

``-v``
  Increase verbosity, useful for debugging the tool. It can be repeated to
  increase more the verbosity.

.. _option_V_:

``-V <virtual>``
  Load a virtual USB device configuration, to use a virtualized FTDI/EEPROM
  environment. This is useful for PyFtdi_ development, and to test EEPROM
  configuration with a virtual setup. This option is not useful for regular
  usage. See :ref:`virtual_framework`.

