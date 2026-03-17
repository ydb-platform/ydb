
.. include:: ../defs.rst

:mod:`eeprom` - EEPROM API
--------------------------

.. module :: pyftdi.eeprom


Quickstart
~~~~~~~~~~

Example: dump the EEPROM content

.. code-block:: python

    # Instantiate an EEPROM manager
    eeprom = FtdiEeprom()

    # Select the FTDI device to access (the interface is mandatory but any
    # valid interface for the device fits)
    eeprom.open('ftdi://ftdi:2232h/1')

    # Show the EEPROM content
    eeprom.dump_config()

    # Show the raw EEPROM content
    from pyftdi.misc import hexdump
    print(hexdump(eeprom.data))


Example: update the serial number

.. code-block:: python

    # Instantiate an EEPROM manager
    eeprom = FtdiEeprom()

    # Select the FTDI device to access
    eeprom.open('ftdi://ftdi:2232h/1')

    # Change the serial number
    eeprom.set_serial_number('123456')

    # Commit the change to the EEPROM
    eeprom.commit(dry_run=False)


Classes
~~~~~~~

.. autoclass :: FtdiEeprom
 :members:


Exceptions
~~~~~~~~~~

.. autoexception :: FtdiEepromError


Tests
~~~~~

.. code-block:: shell

   # optional: specify an alternative FTDI device
   export FTDI_DEVICE=ftdi://ftdi:2232h/1
   PYTHONPATH=. python3 pyftdi/tests/eeprom.py
