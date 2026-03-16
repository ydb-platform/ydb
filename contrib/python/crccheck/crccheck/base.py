""" Base class for CRC and checksum classes.

  License::

    MIT License

    Copyright (c) 2015-2022 by Martin Scharrer <martin.scharrer@web.de>

    Permission is hereby granted, free of charge, to any person obtaining a copy of this software
    and associated documentation files (the "Software"), to deal in the Software without
    restriction, including without limitation the rights to use, copy, modify, merge, publish,
    distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the
    Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all copies or
    substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
    BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
    NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
    DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

"""
import math

REFLECT_BIT_ORDER_TABLE = bytes((
    0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0,
    0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0,
    0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8,
    0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8,
    0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4,
    0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
    0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC,
    0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC,
    0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2,
    0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2,
    0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA,
    0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
    0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6,
    0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6,
    0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE,
    0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
    0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1,
    0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
    0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9,
    0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9,
    0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5,
    0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
    0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED,
    0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
    0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3,
    0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3,
    0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB,
    0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
    0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7,
    0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
    0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF,
    0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF,
))


def reflectbitorder(width, value):
    """ Reflects the bit order of the given value according to the given bit width.

        Args:
            width (int): bitwidth
            value (int): value to reflect
    """
    binstr = ("0" * width + bin(value)[2:])[-width:]
    return int(binstr[::-1], 2)


class CrccheckError(Exception):
    """General checksum error exception"""
    pass


class CrccheckBase(object):
    """ Abstract base class for checksumming classes.

        Args:
            initvalue (int): Initial value. If None then the default value for the class is used.
    """
    _initvalue = 0x00
    _check_result = None
    _check_data = None
    _width = 0

    def __init__(self, initvalue=None, **kwargs):
        if initvalue is None:
            self._value = self._initvalue
        else:
            self._value = initvalue

    @classmethod
    def initvalue(cls):
        """Getter for initvalue."""
        return cls._initvalue

    @classmethod
    def check_result(cls):
        """Getter for check_result."""
        return cls._check_result

    @classmethod
    def check_data(cls):
        """Getter for check_data."""
        return cls._check_data

    @classmethod
    def width(cls):
        return cls._width

    @classmethod
    def bytewidth(cls):
        return (cls._width + 7) // 8

    def reset(self, value=None):
        """ Reset instance.

            Resets the instance state to the initial value.
            This is not required for a just created instance.

            Args:
                value (int): Set internal value. If None then the default initial value for the class is used.

            Returns:
                self
        """
        if value is None:
            self._value = self._initvalue
        else:
            self._value = value
        return self

    def process(self, data):
        """ Process given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.

            Returns:
                self
        """
        raise NotImplementedError

    def final(self):
        """Return final check value.
           The internal state is not modified by this so further data can be processed afterwards.

           Return:
               int: final value
        """
        return self._value

    def finalhex(self, byteorder='big'):
        """Return final checksum value as hexadecimal string (without leading "0x").
           The hex value is zero padded to bitwidth/8.
           The internal state is not modified by this so further data can be processed afterwards.

           Return:
               str: final value as hex string without leading '0x'.
        """
        asbytes = self.finalbytes(byteorder)
        try:
            # bytearray.hex() is new in Python 3.5
            return asbytes.hex()
        except AttributeError:  # pragma: no cover
            return "".join(["{:02x}".format(b) for b in asbytes])

    def finalbytes(self, byteorder='big'):
        """Return final checksum value as bytes.
           The internal state is not modified by this so further data can be processed afterwards.

           Return:
               bytes: final value as bytes
        """
        bytelength = int(math.ceil(self._width / 8.0))
        asint = self.final()
        try:
            # int.to_bytes() is new in Python 3.2
            return asint.to_bytes(bytelength, byteorder)
        except AttributeError:  # pragma: no cover
            asbytes = bytearray(bytelength)
            for i in range(0, bytelength):
                asbytes[i] = asint & 0xFF
                asint >>= 8
            if byteorder == 'big':
                asbytes.reverse()
            return asbytes

    def value(self):
        """Returns current intermediate value.
           Note that in general final() must be used to get the final value.

           Return:
               int: current value
        """
        return self._value

    @classmethod
    def calc(cls, data, initvalue=None, **kwargs):
        """ Fully calculate CRC/checksum over given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.
                initvalue (int): Initial value. If None then the default value for the class is used.

            Return:
                int: final value
        """
        inst = cls(initvalue, **kwargs)
        inst.process(data)
        return inst.final()

    @classmethod
    def calchex(cls, data, initvalue=None, byteorder='big', **kwargs):
        """Fully calculate checksum over given data. Return result as hex string.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.
                initvalue (int): Initial value. If None then the default value for the class is used.
                byteorder ('big' or 'little'): order (endianness) of returned bytes.

            Return:
                str: final value as hex string without leading '0x'.
        """
        inst = cls(initvalue, **kwargs)
        inst.process(data)
        return inst.finalhex(byteorder)

    @classmethod
    def calcbytes(cls, data, initvalue=None, byteorder='big', **kwargs):
        """Fully calculate checksum over given data. Return result as bytearray.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.
                initvalue (int): Initial value. If None then the default value for the class is used.
                byteorder ('big' or 'little'): order (endianness) of returned bytes.

            Return:
                bytes: final value as bytes
        """
        inst = cls(initvalue, **kwargs)
        inst.process(data)
        return inst.finalbytes(byteorder)

    @classmethod
    def selftest(cls, data=None, expectedresult=None, **kwargs):
        """ Selftest method for automated tests.

            Args:
                data (bytes, bytearray or list of int [0-255]): data to process
                expectedresult (int): expected result

            Raises:
                CrccheckError: if result is not as expected
        """
        if data is None:
            data = cls._check_data
            expectedresult = cls._check_result
        result = cls.calc(data, **kwargs)
        if result != expectedresult:
            raise CrccheckError("{:s}: expected {:s}, got {:s}".format(cls.__name__, hex(expectedresult), hex(result)))

    def copy(self):
        """Creates a copy of the Crc/Checksum instance. This can be used to efficiently compute the CRC/checksum of data
           sharing common initial part."""
        import copy
        return copy.deepcopy(self)
