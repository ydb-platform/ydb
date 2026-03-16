""" Classes to calculated additive and XOR checksums.

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
from crccheck.base import CrccheckBase, CrccheckError


class ChecksumBase(CrccheckBase):
    """ Base class for all checksum classes.

        Args:
            initvalue (int): Initial value. If None then the default value for the class is used.
            byteorder ('big' or 'little'): byte order (endianness) used when reading the input bytes.
    """
    _width = 0
    _mask = 0
    _check_data = (0xDE, 0xAD, 0xBE, 0xEF, 0xAA, 0x55, 0xC2, 0x8C)
    _check_result_littleendian = None

    def __init__(self, initvalue=0, byteorder='big'):
        super(ChecksumBase, self).__init__(initvalue)
        self._byteorder = byteorder

    @classmethod
    def mask(cls):
        """Getter for mask."""
        return cls._mask

    @classmethod
    def check_result(cls, byteorder='big'):
        """
        Getter for check_result.
        Args:
            byteorder: Either 'big' (default) or 'little'.
                       Should only be used as a keyword argument for upwards compatiblity.
        """
        if byteorder == 'big':
            return cls._check_result
        else:
            return cls._check_result_littleendian

    def process(self, data):
        """ Process given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.

            Returns:
                self
        """
        dataword = 0
        n = 0
        bigendian = (self._byteorder == 'big')
        width = self._width
        mask = self._mask
        value = self._value
        for byte in data:
            if bigendian:
                dataword = (dataword << 8) | byte
            else:
                dataword |= (byte << n)
            n += 8
            if n == width:
                value = mask & (value + dataword)
                dataword = 0
                n = 0
        self._value = value
        return self

    @classmethod
    def selftest(cls, data=None, expectedresult=None, byteorder='big'):
        """ Selftest method for automated tests.

            Args:
                data (bytes, bytearray or list of int [0-255]): data to process
                expectedresult (int): expected result
                byteorder ('big' or 'little'): byte order (endianness) used when reading the input bytes.

            Raises:
                CrccheckError: if result is not as expected
        """
        if data is None:
            data = cls._check_data
        if expectedresult is None:
            if byteorder == 'big':
                expectedresult = cls._check_result
            else:
                expectedresult = cls._check_result_littleendian
        result = cls.calc(data, byteorder=byteorder)
        if result != expectedresult:
            raise CrccheckError(hex(result))


class Checksum32(ChecksumBase):
    """ 32-bit checksum.

        Calculates 32-bit checksum by adding the input bytes in groups of four.
        Input data length must be a multiple of four, otherwise the last bytes are not used.
    """
    _width = 32
    _mask = 0xFFffFFff
    _check_result = 0x8903817B
    _check_result_littleendian = 0x7C810388


class Checksum16(ChecksumBase):
    """ 16-bit checksum.

        Calculates 16-bit checksum by adding the input bytes in groups of two.
        Input data length must be a multiple of two, otherwise the last byte is not used.
    """
    _width = 16
    _mask = 0xFFff
    _check_result = 0x0A7D
    _check_result_littleendian = 0x8008


class Checksum8(ChecksumBase):
    """ 8-bit checksum.

        Calculates 8-bit checksum by adding the input bytes.
    """
    _width = 8
    _mask = 0xFF
    _check_result = 0x85
    _check_result_littleendian = _check_result


class ChecksumXorBase(ChecksumBase):
    """ Base class for all XOR checksum classes. """

    def process(self, data):
        """ Process given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.

            Returns:
                self
        """
        dataword = 0
        n = 0
        bigendian = (self._byteorder == 'big')
        width = self._width
        mask = self._mask
        value = self._value
        for byte in data:
            if bigendian:
                dataword = (dataword << 8) | byte
            else:
                dataword |= (byte << n)
            n += 8
            if n == width:
                value = mask & (value ^ dataword)
                dataword = 0
                n = 0
        self._value = value
        return self


class ChecksumXor32(ChecksumXorBase):
    """ 32-bit XOR checksum.

        Calculates 32-bit checksum by XOR-ing the input bytes in groups of four.
        Input data length must be a multiple of four, otherwise the last bytes are not used.
    """
    _width = 32
    _mask = 0xFFffFFff
    _check_result = 0x74F87C63
    _check_result_littleendian = 0x637CF874


class ChecksumXor16(ChecksumXorBase):
    """ 16-bit XOR checksum.

        Calculates 16-bit checksum by XOR-ing the input bytes in groups of two.
        Input data length must be a multiple of two, otherwise the last byte is not used.
    """
    _width = 16
    _mask = 0xFFff
    _check_result = 0x089B
    _check_result_littleendian = 0x9B08


class ChecksumXor8(ChecksumXorBase):
    """ 8-bit XOR checksum.

        Calculates 8-bit checksum by XOR-ing the input bytes.
    """
    _width = 8
    _mask = 0xFF
    _check_result = 0x93
    _check_result_littleendian = _check_result


class Checksum(ChecksumBase):
    """ General additive checksum.

        Args:
            width (int): bit width of checksum. Must be positive and a multiple of 8.
            initvalue (int): Initial value. If None then the default value for the class is used.
            byteorder ('big' or 'little'): byte order (endianness) used when reading the input bytes.
    """
    _check_result = None
    _check_result_littleendian = None

    def __init__(self, width, initvalue=0, byteorder='big'):
        super(Checksum, self).__init__(initvalue, byteorder)
        width = int(width)
        if width <= 0 or width % 8 != 0:
            raise ValueError("width must be postive and a multiple of 8")
        self._width = width
        self._mask = (1 << width) - 1


class ChecksumXor(ChecksumXorBase):
    """ General XOR checksum.

        Args:
            width (int): bit width of checksum. Must be positive and a multiple of 8.
            initvalue (int): Initial value. If None then the default value for the class is used.
            byteorder ('big' or 'little'): byte order (endianness) used when reading the input bytes.
    """
    _check_result = None
    _check_result_littleendian = None

    def __init__(self, width, initvalue=0, byteorder='big'):
        super(ChecksumXor, self).__init__(initvalue, byteorder)
        width = int(width)
        if width <= 0 or width % 8 != 0:
            raise ValueError("width must be postive and a multiple of 8")
        self._width = width
        self._mask = (1 << width) - 1


ALLCHECKSUMCLASSES = (
    Checksum8, Checksum16, Checksum32,
    ChecksumXor8, ChecksumXor16, ChecksumXor32,
)
