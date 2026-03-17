.. include:: defs.rst

FTDI device pinout
------------------

============ ============= ======= ====== ============== ========== ====== =============
 IF/1 [#ih]_ IF/2 [#if2]_  BitBang  UART   |I2C|          SPI        JTAG   C232HD cable
============ ============= ======= ====== ============== ========== ====== =============
 ``ADBUS0``   ``BDBUS0``   GPIO0    TxD    SCK            SCLK       TCK   Orange
 ``ADBUS1``   ``BDBUS1``   GPIO1    RxD    SDA/O [#i2c]_  MOSI       TDI   Yellow
 ``ADBUS2``   ``BDBUS2``   GPIO2    RTS    SDA/I [#i2c]_  MISO       TDO   Green
 ``ADBUS3``   ``BDBUS3``   GPIO3    CTS    GPIO3          CS0        TMS   Brown
 ``ADBUS4``   ``BDBUS4``   GPIO4    DTR    GPIO4          CS1/GPIO4        Grey
 ``ADBUS5``   ``BDBUS5``   GPIO5    DSR    GPIO5          CS2/GPIO5        Purple
 ``ADBUS6``   ``BDBUS6``   GPIO6    DCD    GPIO6          CS3/GPIO6        White
 ``ADBUS7``   ``BDBUS7``   GPIO7    RI     RSCK [#rck]_   CS4/GPIO7  RCLK  Blue
 ``ACBUS0``   ``BCBUS0``                   GPIO8          GPIO8
 ``ACBUS1``   ``BCBUS1``                   GPIO9          GPIO9
 ``ACBUS2``   ``BCBUS2``                   GPIO10         GPIO10
 ``ACBUS3``   ``BCBUS3``                   GPIO11         GPIO11
 ``ACBUS4``   ``BCBUS4``                   GPIO12         GPIO12
 ``ACBUS5``   ``BCBUS5``                   GPIO13         GPIO13
 ``ACBUS6``   ``BCBUS6``                   GPIO14         GPIO14
 ``ACBUS7``   ``BCBUS7``                   GPIO15         GPIO15
============ ============= ======= ====== ============== ========== ====== =============

.. [#ih]  16-bit port (ACBUS, BCBUS) is not available with FT4232H_ series, and
          FTDI2232C/D only support 12-bit ports.
.. [#i2c] FTDI pins are either configured as input or output. As |I2C| SDA line
          is bi-directional, two FTDI pins are required to provide the SDA
          feature, and they should be connected together and to the SDA |I2C|
          bus line. Pull-up resistors on SCK and SDA lines should be used.
.. [#if2] FT232H_ does not support a secondary MPSSE port, only FT2232H_,
          FT4232H_ and FT4232HA_ do. Note that FT4232H_/FT4232HA_ has 4 serial
          ports, but only the first two interfaces are MPSSE-capable. C232HD
          cable only exposes IF/1 (ADBUS).
.. [#rck] In order to support I2C clock stretch mode, ADBUS7 should be
          connected to SCK. When clock stretching mode is not selected, ADBUS7
          may be used as GPIO7.