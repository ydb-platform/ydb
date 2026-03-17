.. include:: defs.rst

Troubleshooting
---------------

Reporting a bug
~~~~~~~~~~~~~~~

Please do not contact the author by email. The preferered method to report bugs
and/or enhancement requests is through
`GitHub <https://github.com/eblot/pyftdi/issues>`_.

Please be sure to read the next sections before reporting a new issue.

Logging
~~~~~~~

FTDI uses the `pyftdi` logger.

It emits log messages with raw payload bytes at DEBUG level, and data loss
at ERROR level.

Common error messages
~~~~~~~~~~~~~~~~~~~~~

"Error: No backend available"
`````````````````````````````

libusb native library cannot be loaded. Try helping the dynamic loader:

* On Linux: ``export LD_LIBRARY_PATH=<path>``

  where ``<path>`` is the directory containing the ``libusb-1.*.so``
  library file

* On macOS: ``export DYLD_LIBRARY_PATH=.../lib``

  where ``<path>`` is the directory containing the ``libusb-1.*.dylib``
  library file

* On Windows:

  Try to copy the USB dll where the Python executable is installed, along
  with the other Python DLLs.

  If this happens while using an exe created by pyinstaller:
  ``copy C:\Windows\System32\libusb0.dll <path>``

  where ``<path>`` is the directory containing the executable created
  by pyinstaller. This assumes you have installed libusb (using a tool
  like Zadig) as referenced in the installation guide for Windows.


"Error: Access denied (insufficient permissions)"
`````````````````````````````````````````````````

The system may already be using the device.

* On macOS: starting with 10.9 "*Mavericks*", macOS ships with a native FTDI
  kernel extension that preempts access to the FTDI device.

  Up to 10.13 "*High Sierra*", this driver can be unloaded this way:

  .. code-block:: shell

      sudo kextunload [-v] -bundle com.apple.driver.AppleUSBFTDI

  You may want to use an alias or a tiny script such as
  ``pyftdi/bin/uphy.sh``

  Please note that the system automatically reloads the driver, so it may be
  useful to move the kernel extension so that the system never loads it.

  .. warning::

     From macOS 10.14 "*Mojave*", the Apple kernel extension peacefully
     co-exists with libusb_ and PyFtdi_, so you no longer need - and **should
     not attempt** - to unload the kernel extension. If you still experience
     this error, please verify you have not installed another driver from FTDI,
     such as FTDI's D2XX.

* On Linux: it may indicate a missing or invalid udev configuration. See
  the :doc:`installation` section.

* This error message may also be triggered whenever the communication port is
  already in use.


"Error: The device has no langid"
`````````````````````````````````

* On Linux, it usually comes from the same installation issue as the
  ``Access denied`` error: the current user is not granted the permissions to
  access the FTDI device, therefore pyusb cannot read the FTDI registers. Check
  out the :doc:`installation` section.


"Bus error / Access violation"
``````````````````````````````

PyFtdi does not use any native library, but relies on PyUSB_ and libusb_. The
latter uses native code that may trigger OS error. Some early development
versions of libusb_, for example 1.0.22-b…, have been reported to trigger
such issues. Please ensure you use a stable/final versions of libusb_ if you
experience this kind of fatal error.


"serial.serialutil.SerialException: Unable to open USB port"
````````````````````````````````````````````````````````````

May be caused by a conflict with the FTDI virtual COM port (VCOM). Try
uninstalling the driver. On macOS, refer to this `FTDI macOS guide`_.


Slow initialisation on OS X El Capitan
``````````````````````````````````````

It may take several seconds to open or enumerate FTDI devices.

If you run libusb <= v1.0.20, be sure to read the `Libusb issue on macOS`_
with OS X 10.11+.

