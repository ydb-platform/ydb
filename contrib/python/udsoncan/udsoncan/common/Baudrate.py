__all__ = ['Baudrate']

import struct


class Baudrate:
    """
    Represents a link speed in bit per seconds (or symbol per seconds to be more accurate).
    This class is used by the :ref:`LinkControl<LinkControl>` service that controls the underlying protocol speeds.

    The class can encode the baudrate in 2 different fashions : **Fixed** or **Specific**.

    Some standard baudrate values are defined within ISO-14229:2006 Annex B.3

    :param baudrate: The baudrate to be used. 
    :type baudrate: int

    :param baudtype: Tells how the baudrate shall be encoded. 4 values are possible:

            - ``Baudrate.Type.Fixed`` (0) : Will encode the baudrate in a single byte Fixed fashion. `baudrate` should be a supported value such as 9600, 19200, 125000, 250000, etc.
            - ``Baudrate.Type.Specific`` (1) : Will encode the baudrate in a three-byte Specific fashion. `baudrate` can be any value ranging from 0 to 0xFFFFFF
            - ``Baudrate.Type.Identifier`` (2) : Will encode the baudrate in a single byte Fixed fashion. `baudrate` should be the byte value to encode if the user wants to use a custom type.
            - ``Baudrate.Type.Auto`` (3) : Let the class guess the type. 

                    - If ``baudrate`` is a known standard value (19200, 38400, etc), then Fixed shall be used
                    - If ``baudrate`` is an integer that fits in a single byte, then Identifier shall be used
                    - If ``baudrate`` is none of the above, then Specific will be used.
    :type baudtype: int

    """
    baudrate_map = {
        9600: 0x01,
        19200: 0x02,
        38400: 0x03,
        57600: 0x04,
        115200: 0x05,
        125000: 0x10,
        250000: 0x11,
        500000: 0x12,
        1000000: 0x13,
    }

    class Type:
        Fixed = 0		# When baudrate is a predefined value from standard
        Specific = 1  # When using custom baudrate
        Identifier = 2  # Baudrate implied by baudrate ID
        Auto = 3		# Let the class decide the type

    baudtype: int
    baudrate: int

    # User can specify the type of baudrate or let this class guess what he wants (this adds some simplicity for non-experts).
    def __init__(self, baudrate: int, baudtype: int = Type.Auto):
        if not isinstance(baudrate, int):
            raise ValueError('baudrate must be an integer')

        if baudrate < 0:
            raise ValueError('baudrate must be an integer greater than 0')

        if baudtype == self.Type.Auto:
            if baudrate in self.baudrate_map:
                self.baudtype = self.Type.Fixed
            else:
                if baudrate <= 0xFF:
                    self.baudtype = self.Type.Identifier
                else:
                    self.baudtype = self.Type.Specific
        else:
            self.baudtype = baudtype

        if self.baudtype == self.Type.Specific:
            if baudrate > 0xFFFFFF:
                raise ValueError('Baudrate value cannot be bigger than a 24 bits value.')

        elif self.baudtype == self.Type.Identifier:
            if baudrate > 0xFF:
                raise ValueError('Baudrate ID must be an integer between 0 and 0xFF')
        elif self.baudtype == self.Type.Fixed:
            if baudrate not in self.baudrate_map:
                raise ValueError('baudrate must be part of the supported baudrate list defined by UDS standard')
        else:
            raise ValueError('Unknown baudtype : %s' % self.baudtype)

        self.baudrate = baudrate

    # internal helper to change the type of this baudrate
    def make_new_type(self, baudtype: int) -> "Baudrate":
        if baudtype not in [self.Type.Fixed, self.Type.Specific]:
            raise ValueError('Baudrate type can only be change to Fixed or Specific')

        return Baudrate(self.effective_baudrate(), baudtype=baudtype)

    # Returns the baudrate in Symbol Per Seconds if available, otherwise value given by the user.
    def effective_baudrate(self) -> int:
        if self.baudtype == self.Type.Identifier:
            for k in self.baudrate_map:
                if self.baudrate_map[k] == self.baudrate:
                    return k

            raise RuntimeError('Unknown effective baudrate, this could indicate a bug')
        else:
            return self.baudrate

    # Encodes the baudrate value the way they are exchanged.
    def get_bytes(self) -> bytes:
        if self.baudtype == self.Type.Fixed:
            return struct.pack('B', self.baudrate_map[self.baudrate])

        if self.baudtype == self.Type.Specific:
            b1 = (self.baudrate >> 16) & 0xFF
            b2 = (self.baudrate >> 8) & 0xFF
            b3 = (self.baudrate >> 0) & 0xFF
            return struct.pack('BBB', b1, b2, b3)

        if self.baudtype == self.Type.Identifier:
            return struct.pack('B', self.baudrate)

        raise RuntimeError('Unknown baudrate baudtype : %s' % self.baudtype)

    def __str__(self) -> str:
        baudtype_str = ''
        if self.baudtype == self.Type.Fixed:
            baudtype_str = 'Fixed'
        elif self.baudtype == self.Type.Specific:
            baudtype_str = 'Specific'
        elif self.baudtype == self.Type.Identifier:
            baudtype_str = 'Defined by identifier'

        return '%sBauds, %s format.' % (str(self.effective_baudrate()), baudtype_str)

    def __repr__(self):
        return '<%s: %s at 0x%08x>' % (self.__class__.__name__, str(self), id(self))
