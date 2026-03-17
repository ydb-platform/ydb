This package provides a Python module to control the GPIO on a Raspberry Pi.

Note that this module is unsuitable for real-time or timing critical applications.  This is because you
can not predict when Python will be busy garbage collecting.  It also runs under the Linux kernel which
is not suitable for real time applications - it is multitasking O/S and another process may be given
priority over the CPU, causing jitter in your program.  If you are after true real-time performance and
predictability, buy yourself an Arduino http://www.arduino.cc !

Note that the current release does not support SPI, I2C, hardware PWM or serial functionality on the RPi yet.
This is planned for the near future - watch this space!  One-wire functionality is also planned.

Although hardware PWM is not available yet, software PWM is available to use on all channels.

For examples and documentation, visit http://sourceforge.net/p/raspberry-gpio-python/wiki/Home/

