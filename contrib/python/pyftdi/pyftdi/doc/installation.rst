.. include:: defs.rst

Installation
------------

Prerequisites
~~~~~~~~~~~~~

PyFTDI_ relies on PyUSB_, which requires a native dependency: libusb 1.x.

The actual command to install depends on your OS and/or your distribution,
see below

.. _install_linux:

Debian/Ubuntu Linux
```````````````````

.. code-block:: shell

     apt-get install libusb-1.0

On Linux, you also need to create a `udev` configuration file to allow
user-space processes to access to the FTDI devices. There are many ways to
configure `udev`, here is a typical setup:

::

    # /etc/udev/rules.d/11-ftdi.rules

    # FT232AM/FT232BM/FT232R
    SUBSYSTEM=="usb", ATTR{idVendor}=="0403", ATTR{idProduct}=="6001", GROUP="plugdev", MODE="0664"
    # FT2232C/FT2232D/FT2232H
    SUBSYSTEM=="usb", ATTR{idVendor}=="0403", ATTR{idProduct}=="6010", GROUP="plugdev", MODE="0664"
    # FT4232/FT4232H
    SUBSYSTEM=="usb", ATTR{idVendor}=="0403", ATTR{idProduct}=="6011", GROUP="plugdev", MODE="0664"
    # FT232H
    SUBSYSTEM=="usb", ATTR{idVendor}=="0403", ATTR{idProduct}=="6014", GROUP="plugdev", MODE="0664"
    # FT230X/FT231X/FT234X
    SUBSYSTEM=="usb", ATTR{idVendor}=="0403", ATTR{idProduct}=="6015", GROUP="plugdev", MODE="0664"
    # FT4232HA
    SUBSYSTEM=="usb", ATTR{idVendor}=="0403", ATTR{idProduct}=="6048", GROUP="plugdev", MODE="0664"

.. note:: **Accessing FTDI devices with custom VID/PID**

   You need to add a line for each device with a custom VID / PID pair you
   declare, see :ref:`custom_vid_pid` for details.

You need to unplug / plug back the FTDI device once this file has been
created so that `udev` loads the rules for the matching device, or
alternatively, inform the ``udev`` daemon about the changes:

.. code-block:: shell

   sudo udevadm control --reload-rules
   sudo udevadm trigger

With this setup, be sure to add users that want to run PyFtdi_ to the
`plugdev` group, *e.g.*

.. code-block:: shell

    sudo adduser $USER plugdev

Remember that you need to log out / log in to get the above command
effective, or start a subshell to try testing PyFtdi_:

.. code-block:: shell

    newgrp plugdev


.. _install_macos:

Homebrew macOS
``````````````

.. code-block:: shell

    brew install libusb


.. _install_windows:

Windows
```````

Windows is not officially supported (*i.e.* not tested) but some users have
reported successful installations. Windows requires a specific libusb backend
installation.

Zadig
.....

The probably easiest way to deal with libusb on Windows is to use Zadig_

1. Start up the Zadig utility

2. Select ``Options/List All Devices``, then select the FTDI devices you want
   to communicate with. Its names depends on your hardware, *i.e.* the name
   stored in the FTDI EEPROM.

  * With FTDI devices with multiple channels, such as FT2232 (2 channels) and
    FT4232 (4 channels), you **must** install the driver for the composite
    parent, **not** for the individual interfaces. If you install the driver
    for each interface, each interface will be presented as a unique FTDI
    device and you may have difficulties to select a specific FTDI device port
    once the installation is completed. To make the composite parents to appear
    in the device list, uncheck the ``Options/Ignore Hubs or Composite Parents``
    menu item.

  * Be sure to select the parent device, *i.e.* the device name should not end
    with *(Interface N)*, where *N* is the channel number.

    * for example *Dual RS232-HS* represents the composite parent, while
      *Dual RS232-HS (Interface 0)* represents a single channel of the FTDI
      device. Always select the former.

3. Select ``libusb-win32`` (not ``WinUSB``) in the driver list.

4. Click on ``Replace Driver``

See also `Libusb on Windows`_


.. _install_python:

Python
~~~~~~

Python dependencies
```````````````````

Dependencies should be automatically installed with PIP.

  * pyusb >= 1.0.0, != 1.2.0
  * pyserial >= 3.0

Do *not* install PyUSB_ from GitHub development branch (``master``, ...).
Always prefer a stable, tagged release.

PyUSB 1.2.0 also broke the backward compatibility of the Device API, so it will
not work with PyFtdi.

Installing with PIP
```````````````````

PIP should automatically install the missing dependencies.

.. code-block:: shell

     pip3 install pyftdi


.. _install_from_source:

Installing from source
``````````````````````

If you prefer to install from source, check out a fresh copy from PyFtdi_
github repository.

.. code-block:: shell

     git clone https://github.com/eblot/pyftdi.git
     cd pyftdi
     # note: 'pip3' may simply be 'pip' on some hosts
     pip3 install -r requirements.txt
     python3 setup.py install


.. _generate_doc:

Generating the documentation
````````````````````````````

Follow :ref:`install_from_source` then:

.. code-block:: shell

     pip3 install setuptools wheel sphinx sphinx_autodoc_typehints
     # Shpinx Read the Doc theme seems to never get a release w/ fixed issues
     pip3 install -U -e git+https://github.com/readthedocs/sphinx_rtd_theme.git@2b8717a3647cc650625c566259e00305f7fb60aa#egg=sphinx_rtd_theme
     sphinx-build -b html pyftdi/doc .

The documentation may be accessed from the generated ``index.html`` entry file.


Post-installation sanity check
``````````````````````````````

Open a *shell*, or a *CMD* on Windows

.. code-block:: shell

    python3  # or 'python' on Windows
    from pyftdi.ftdi import Ftdi
    Ftdi.show_devices()

should list all the FTDI devices available on your host.

Alternatively, you can invoke ``ftdi_urls.py`` script that lists all detected
FTDI devices. See the :doc:`tools` chapter for details.

  * Example with 1 FT232H device with a serial number and 1 FT2232 device
    with no serial number, connected to the host:

    .. code-block::

        Available interfaces:
          ftdi://ftdi:232h:FT1PWZ0Q/1   (C232HD-DDHSP-0)
          ftdi://ftdi:2232/1            (Dual RS232-HS)
          ftdi://ftdi:2232/2            (Dual RS232-HS)


Note that FTDI devices with custom VID/PID are not listed with this simple
command, please refer to the PyFtdi_ API to add custom identifiers, *i.e.* see
:py:meth:`pyftdi.ftdi.Ftdi.add_custom_vendor` and
:py:meth:`pyftdi.ftdi.Ftdi.add_custom_product` APIs.


.. _custom_vid_pid:

Custom USB vendor and product IDs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

PyFtdi only recognizes FTDI official vendor and product IDs.

If you have an FTDI device with an EEPROM with customized IDs, you need to tell
PyFtdi to support those custom USB identifiers.

Custom PID
``````````

To support a custom product ID (16-bit integer) with the official FTDI ID, add
the following code **before** any call to an FTDI ``open()`` method.

.. code-block:: python

   from pyftdi.ftdi import Ftdi

   Ftdi.add_custom_product(Ftdi.DEFAULT_VENDOR, product_id)

Custom VID
``````````

To support a custom vendor ID and product ID (16-bit integers), add the
following code **before** any call to an FTDI ``open()`` method.

.. code-block:: python

   from pyftdi.ftdi import Ftdi

   Ftdi.add_custom_vendor(vendor_id)
   Ftdi.add_custom_product(vendor_id, product_id)

You may also specify an arbitrary string to each method if you want to specify
a URL by custom vendor and product names instead of their numerical values:

.. code-block:: python

   from pyftdi.ftdi import Ftdi

   Ftdi.add_custom_vendor(0x1234, 'myvendor')
   Ftdi.add_custom_product(0x1234, 0x5678, 'myproduct')

   f1 = Ftdi.create_from_url('ftdi://0x1234:0x5678/1')
   f2 = Ftdi.create_from_url('ftdi://myvendor:myproduct/2')

.. note::

   Remember that on OSes that require per-device access permissions such as
   Linux, you also need to add the custom VID/PID entry to the configuration
   file, see :ref:`Linux installation <install_linux>` ``udev`` rule file.
