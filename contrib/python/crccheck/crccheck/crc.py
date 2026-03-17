""" Classes to calculate CRCs (Cyclic Redundancy Check).

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
from crccheck.base import CrccheckBase, reflectbitorder, REFLECT_BIT_ORDER_TABLE, CrccheckError


class CrcBase(CrccheckBase):
    """Abstract base class for all Cyclic Redundancy Checks (CRC) checksums"""
    _names = ()
    _width = 0
    _poly = 0x00
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = None
    _check_data = bytearray(b"123456789")
    _residue = None

    @classmethod
    def poly(cls):
        """Getter for polynominal value."""
        return cls._poly

    @classmethod
    def reflect_input(cls):
        """Getter for reflect_input."""
        return cls._reflect_input

    @classmethod
    def reflect_output(cls):
        """Getter for reflect_output."""
        return cls._reflect_output

    @classmethod
    def xor_output(cls):
        """Getter for xor_output value."""
        return cls._xor_output

    @classmethod
    def residue(cls):
        """Getter for residue value."""
        return cls._residue

    def process(self, data):
        """ Process given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.

            Returns:
                self
        """
        crc = self._value
        highbit = 1 << (self._width - 1)
        mask = (1 << self._width) - 1
        poly = self._poly
        shift = self._width - 8
        diff8 = -shift
        if diff8 > 0:
            # enlarge temporary to fit 8-bit
            mask = 0xFF
            crc <<= diff8
            shift = 0
            highbit = 0x80
            poly = self._poly << diff8

        reflect = self._reflect_input
        for byte in data:
            if reflect:
                byte = REFLECT_BIT_ORDER_TABLE[byte]
            crc ^= (byte << shift)
            for i in range(0, 8):
                if crc & highbit:
                    crc = (crc << 1) ^ poly
                else:
                    crc = (crc << 1)
            crc &= mask
        if diff8 > 0:
            crc >>= diff8
        self._value = crc
        return self

    def final(self):
        """ Return final CRC value.

            Return:
                int: final CRC value
        """
        crc = self._value
        if self._reflect_output:
            crc = reflectbitorder(self._width, crc)
        crc ^= self._xor_output
        return crc

    def __eq__(self, other):
        # noinspection PyProtectedMember
        return self._width == other.width() and \
               self._poly == other.poly() and \
               self._initvalue == other.initvalue() and \
               self._reflect_input == other.reflect_input() and \
               self._reflect_output == other.reflect_output() and \
               self._xor_output == other.xor_output()

    def __repr__(self):
        residue = hex(self._residue) if self._residue is not None else 'None'
        check_result = hex(self._check_result) if self._check_result is not None else 'None'
        return ("Crc(width={:d}, poly=0x{:x}, initvalue=0x{:X}, reflect_input={!s:s}, reflect_output={!s:s}, " +
                "xor_output=0x{:x}, check_result={}, residue={})").format(
            self._width, self._poly, self._initvalue, self._reflect_input, self._reflect_output,
            self._xor_output, check_result, residue)


def find(classes=None, width=None, poly=None, initvalue=None, reflect_input=None, reflect_output=None, xor_output=None,
         check_result=None, residue=None):
    """Find CRC classes which the matching properties.

    Args:
        classes (None or list): List of classes to search in. If None the list ALLCRCCLASSES will be used.
        width (None or int): number of bits of the CRC classes to find
        poly (None or int): polygon to find
        initvalue (None or int): initvalue to find
        reflect_input (None or bool): reflect_input to find
        reflect_output (None or bool): reflect_output to find
        xor_output (None or int): xor_output to find
        check_result (None or int): check_result to find
        residue (None or int): residue to find

    Returns:
        List of CRC classes with the selected properties.

    Examples:
        Find all CRC16 classes:
            $ find(width=16)

        Find all CRC32 classes with all-1 init value and XOR output:
            $ find(width=32, initvalue=0xFFFF, xor_output=0xFFFF)
    """
    found = list()
    if classes is None:
        classes = ALLCRCCLASSES
    for cls in classes:
        if width is not None and width != cls._width:
            continue
        if poly is not None and poly != cls._poly:
            continue
        if initvalue is not None and initvalue != cls._initvalue:
            continue
        if reflect_input is not None and reflect_input != cls._reflect_input:
            continue
        if reflect_output is not None and reflect_output != cls._reflect_output:
            continue
        if xor_output is not None and xor_output != cls._xor_output:
            continue
        if check_result is not None and check_result != cls._check_result:
            continue
        if residue is not None and residue != cls._residue:
            continue
        found.append(cls)
    return found


def identify(data, crc, width=None, classes=None, one=True):
    """
    Identify the used CRC algorithm which was used to calculate the CRC from some data.

    This function can be used to identify a suitable CRC class if the exact CRC algorithm/parameters
    are not known, but a CRC value is known from some data. Note that this function can be quite
    time-consuming on large data, especially if the given width is not known.

    Args:
        data (bytes): Data to compare with the `crc`.
        crc (int): Known CRC of the given `data`.
        width (int or None): Known bit-width of given `crc`.
            Providing the width will speed up the identification of the CRC algorithm.
        classes (iterable or None): Listing of classes to check. If None then ALLCRCCLASSES is used.
        one (bool): If True then only the first found CRC class is returned,
            otherwise a list of all suitable CRC classes.

    Returns:
        If `one` is True:
            CRC class which instances produce the given CRC from the given data.
            If no CRC class could be found `None` is returned.
        If `one` is False:
            List of CRC classes which instances produce the given CRC from the given data.
            The list may be empty.
    """
    if classes is None:
        classes = ALLCRCCLASSES
    if width is not None:
        classes = (cls for cls in classes if cls._width == width)

    found = []
    for cls in classes:
        if cls().calc(data) == crc:
            if one:
                return cls
            found.append(cls)
    if one:
        return None
    return found


class Crc(CrcBase):
    """ Creates a new general (user-defined) CRC calculator instance.

        Arguments:
            width (int): bit-width of CRC.
            poly (int): polynomial of CRC with the top bit omitted.
            initvalue (int): initial value of internal running CRC value. Usually either 0 or (1<<width)-1,
                i.e. "all-1s".
            reflect_input (bool): If true the bit order of the input bytes are reflected first.
                This is to calculate the CRC like least-significant bit first systems will do it.
            reflect_output (bool): If true the bit order of the calculation result will be reflected before
                the XOR output stage.
            xor_output (int): The result is bit-wise XOR-ed with this value. Usually 0 (value stays the same) or
                (1<<width)-1, i.e. "all-1s" (invert value).
            check_result (int): The expected result for the check input "123456789" (= [0x31, 0x32, 0x33, 0x34,
                0x35, 0x36, 0x37, 0x38, 0x39]). This value is used for the selftest() method to verify proper
                operation.
            residue (int): The residue expected after calculating the CRC over the original data followed by the
                CRC of the original data. With initvalue=0 and xor_output=0 the residue calculates always to 0.
    """
    _width = 0
    _poly = 0x00
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = None
    _residue = None

    def __init__(self, width, poly, initvalue=0x00, reflect_input=False, reflect_output=False, xor_output=0x00,
                 check_result=0x00, residue=0x00):
        super(Crc, self).__init__(initvalue)
        self._initvalue = int(initvalue)
        self._width = int(width)
        self._poly = int(poly)
        self._reflect_input = bool(reflect_input)
        self._reflect_output = bool(reflect_output)
        self._xor_output = int(xor_output)
        self._check_result = int(check_result) if check_result is not None else None
        self._residue = int(residue) if residue is not None else None

    def calc(self, data, initvalue=None, **kwargs):
        """ Fully calculate CRC/checksum over given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.
                initvalue (int): Initial value. If None then the default value for the class is used.

            Return:
                int: final value
        """
        self.reset()
        self.process(data)
        return self.final()

    def calchex(self, data, initvalue=None, byteorder='big', **kwargs):
        """Fully calculate checksum over given data. Return result as hex string.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.
                initvalue (int): Initial value. If None then the default value for the class is used.
                byteorder ('big' or 'little'): order (endianness) of returned bytes.

            Return:
                str: final value as hex string without leading '0x'.
        """
        self.reset()
        self.process(data)
        return self.finalhex(byteorder)

    def calcbytes(self, data, initvalue=None, byteorder='big', **kwargs):
        """Fully calculate checksum over given data. Return result as bytearray.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.
                initvalue (int): Initial value. If None then the default value for the class is used.
                byteorder ('big' or 'little'): order (endianness) of returned bytes.

            Return:
                bytes: final value as bytes
        """
        self.reset()
        self.process(data)
        return self.finalbytes(byteorder)

    def selftest(self, data=None, expectedresult=None, **kwargs):
        if data is None:
            data = self._check_data
            expectedresult = self._check_result
        result = self.calc(data)
        if result != expectedresult:
            raise CrccheckError("{:s}: expected {:s}, got {:s}".format(
                self.__class__.__name__, hex(expectedresult), hex(result)))


class Crc8Base(CrcBase):
    """CRC-8.
       Has optimised code for 8-bit CRCs and is used as base class for all other CRC with this width.
    """
    _width = 8
    _poly = 0x07
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0xF4

    def process(self, data):
        """ Process given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.

            Returns:
                self
        """
        crc = self._value

        reflect = self._reflect_input
        poly = self._poly
        for byte in data:
            if reflect:
                byte = REFLECT_BIT_ORDER_TABLE[byte]
            crc = crc ^ byte
            for i in range(0, 8):
                if crc & 0x80:
                    crc = (crc << 1) ^ poly
                else:
                    crc = (crc << 1)
            crc &= 0xFF
        self._value = crc
        return self


class Crc16Base(CrcBase):
    """CRC-16.
       Has optimised code for 16-bit CRCs and is used as base class for all other CRC with this width.
    """
    _width = 16
    _poly = 0x1021
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x31C3

    def process(self, data):
        """ Process given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.

            Returns:
                self
        """
        crc = self._value

        reflect = self._reflect_input
        poly = self._poly
        for byte in data:
            if reflect:
                byte = REFLECT_BIT_ORDER_TABLE[byte]
            crc ^= (byte << 8)
            for i in range(0, 8):
                if crc & 0x8000:
                    crc = (crc << 1) ^ poly
                else:
                    crc = (crc << 1)
            crc &= 0xFFFF
        self._value = crc
        return self


class Crc32Base(CrcBase):
    """CRC-32.
       Has optimised code for 32-bit CRCs and is used as base class for all other CRC with this width.
    """
    _width = 32
    _poly = 0x04C11DB7
    _initvalue = 0xFFFFFFFF
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xFFFFFFFF
    _check_result = 0xCBF43926

    def process(self, data):
        """ Process given data.

            Args:
                data (bytes, bytearray or list of ints [0-255]): input data to process.

            Returns:
                self
        """
        crc = self._value

        reflect = self._reflect_input
        poly = self._poly
        for byte in data:
            if reflect:
                byte = REFLECT_BIT_ORDER_TABLE[byte]
            crc ^= (byte << 24)
            for i in range(0, 8):
                if crc & 0x80000000:
                    crc = (crc << 1) ^ poly
                else:
                    crc = (crc << 1)
            crc &= 0xFFFFFFFF
        self._value = crc
        return self


def _inthex(value):
    try:
        return int(value, 0)
    except TypeError:
        return int(value)


def crccls(width=None, poly=None, initvalue=None, reflect_input=None, reflect_output=None, xor_output=None,
           check_result=None, residue=None, clsname=None, name=None, basecls=None):
    if clsname is None:
        clsname = 'CrcGeneric'
    if name is None:
        name = clsname

    if basecls is not None:
        if not issubclass(basecls, CrcBase):
            raise ValueError('basecls invalid')
        elif width is not None and basecls.width() != width:
            raise ValueError('basecls has unsuitable width')
    elif width is not None:
        basecls = {32: Crc32Base, 16: Crc16Base, 8: Crc8Base}.get(width, CrcBase)
    else:
        raise ValueError('Either width or basecls must be given')

    # Set given attributes only, rest is taken from base class
    attr = {'_names': (str(name),)}
    if width is not None:
        attr['_width'] = _inthex(width)
    if poly is not None:
        attr['_poly'] = _inthex(poly)
    if initvalue is not None:
        attr['_initvalue'] = _inthex(initvalue)
    if reflect_input is not None:
        attr['_reflect_input'] = bool(reflect_input)
    if reflect_output is not None:
        attr['_reflect_output'] = bool(reflect_output)
    if xor_output is not None:
        attr['_xor_output'] = _inthex(xor_output)
    if check_result is not None:
        attr['_check_result'] = _inthex(check_result)
    if residue is not None:
        attr['_residue'] = _inthex(residue)

    return type(str(clsname), (basecls,), attr)


# # # CRC CLASSES # # #


class Crc3Gsm(CrcBase):
    """CRC-3/GSM"""
    _names = ('CRC-3/GSM',)
    _width = 3
    _poly = 0x3
    _initvalue = 0x0
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x7
    _check_result = 0x4
    _residue = 0x2


class Crc3Rohc(CrcBase):
    """CRC-3/ROHC"""
    _names = ('CRC-3/ROHC',)
    _width = 3
    _poly = 0x3
    _initvalue = 0x7
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0
    _check_result = 0x6
    _residue = 0x0


class Crc4G704(CrcBase):
    """CRC-4/G-704

    Aliases: CRC-4/ITU
    """
    _names = ('CRC-4/G-704', 'CRC-4/ITU')
    _width = 4
    _poly = 0x3
    _initvalue = 0x0
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0
    _check_result = 0x7
    _residue = 0x0


Crc4Itu = Crc4G704


class Crc4Interlaken(CrcBase):
    """CRC-4/INTERLAKEN"""
    _names = ('CRC-4/INTERLAKEN',)
    _width = 4
    _poly = 0x3
    _initvalue = 0xf
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xf
    _check_result = 0xb
    _residue = 0x2


class Crc5EpcC1G2(CrcBase):
    """CRC-5/EPC-C1G2

    Aliases: CRC-5/EPC
    """
    _names = ('CRC-5/EPC-C1G2', 'CRC-5/EPC')
    _width = 5
    _poly = 0x09
    _initvalue = 0x09
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x00
    _residue = 0x00


Crc5Epc = Crc5EpcC1G2


class Crc5G704(CrcBase):
    """CRC-5/G-704

    Aliases: CRC-5/ITU
    """
    _names = ('CRC-5/G-704', 'CRC-5/ITU')
    _width = 5
    _poly = 0x15
    _initvalue = 0x00
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x07
    _residue = 0x00


Crc5Itu = Crc5G704


class Crc5Usb(CrcBase):
    """CRC-5/USB"""
    _names = ('CRC-5/USB',)
    _width = 5
    _poly = 0x05
    _initvalue = 0x1f
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x1f
    _check_result = 0x19
    _residue = 0x06


class Crc6Cdma2000A(CrcBase):
    """CRC-6/CDMA2000-A"""
    _names = ('CRC-6/CDMA2000-A',)
    _width = 6
    _poly = 0x27
    _initvalue = 0x3f
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x0d
    _residue = 0x00


class Crc6Cdma2000B(CrcBase):
    """CRC-6/CDMA2000-B"""
    _names = ('CRC-6/CDMA2000-B',)
    _width = 6
    _poly = 0x07
    _initvalue = 0x3f
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x3b
    _residue = 0x00


class Crc6Darc(CrcBase):
    """CRC-6/DARC"""
    _names = ('CRC-6/DARC',)
    _width = 6
    _poly = 0x19
    _initvalue = 0x00
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x26
    _residue = 0x00


class Crc6G704(CrcBase):
    """CRC-6/G-704

    Aliases: CRC-6/ITU
    """
    _names = ('CRC-6/G-704', 'CRC-6/ITU')
    _width = 6
    _poly = 0x03
    _initvalue = 0x00
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x06
    _residue = 0x00


Crc6Itu = Crc6G704


class Crc6Gsm(CrcBase):
    """CRC-6/GSM"""
    _names = ('CRC-6/GSM',)
    _width = 6
    _poly = 0x2f
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x3f
    _check_result = 0x13
    _residue = 0x3a


class Crc7Mmc(CrcBase):
    """CRC-7/MMC

    Aliases: CRC-7
    """
    _names = ('CRC-7/MMC', 'CRC-7')
    _width = 7
    _poly = 0x09
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x75
    _residue = 0x00


Crc7 = Crc7Mmc


class Crc7Rohc(CrcBase):
    """CRC-7/ROHC"""
    _names = ('CRC-7/ROHC',)
    _width = 7
    _poly = 0x4f
    _initvalue = 0x7f
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x53
    _residue = 0x00


class Crc7Umts(CrcBase):
    """CRC-7/UMTS"""
    _names = ('CRC-7/UMTS',)
    _width = 7
    _poly = 0x45
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x61
    _residue = 0x00


class Crc8Autosar(Crc8Base):
    """CRC-8/AUTOSAR"""
    _names = ('CRC-8/AUTOSAR',)
    _width = 8
    _poly = 0x2f
    _initvalue = 0xff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xff
    _check_result = 0xdf
    _residue = 0x42


class Crc8Bluetooth(Crc8Base):
    """CRC-8/BLUETOOTH"""
    _names = ('CRC-8/BLUETOOTH',)
    _width = 8
    _poly = 0xa7
    _initvalue = 0x00
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x26
    _residue = 0x00


class Crc8Cdma2000(Crc8Base):
    """CRC-8/CDMA2000"""
    _names = ('CRC-8/CDMA2000',)
    _width = 8
    _poly = 0x9b
    _initvalue = 0xff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0xda
    _residue = 0x00


class Crc8Darc(Crc8Base):
    """CRC-8/DARC"""
    _names = ('CRC-8/DARC',)
    _width = 8
    _poly = 0x39
    _initvalue = 0x00
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x15
    _residue = 0x00


class Crc8DvbS2(Crc8Base):
    """CRC-8/DVB-S2"""
    _names = ('CRC-8/DVB-S2',)
    _width = 8
    _poly = 0xd5
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0xbc
    _residue = 0x00


class Crc8GsmA(Crc8Base):
    """CRC-8/GSM-A"""
    _names = ('CRC-8/GSM-A',)
    _width = 8
    _poly = 0x1d
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x37
    _residue = 0x00


class Crc8GsmB(Crc8Base):
    """CRC-8/GSM-B"""
    _names = ('CRC-8/GSM-B',)
    _width = 8
    _poly = 0x49
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xff
    _check_result = 0x94
    _residue = 0x53


class Crc8Hitag(Crc8Base):
    """CRC-8/HITAG"""
    _names = ('CRC-8/HITAG',)
    _width = 8
    _poly = 0x1d
    _initvalue = 0xff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0xb4
    _residue = 0x00


class Crc8I4321(Crc8Base):
    """CRC-8/I-432-1

    Aliases: CRC-8/ITU
    """
    _names = ('CRC-8/I-432-1', 'CRC-8/ITU')
    _width = 8
    _poly = 0x07
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x55
    _check_result = 0xa1
    _residue = 0xac


Crc8Itu = Crc8I4321


class Crc8ICode(Crc8Base):
    """CRC-8/I-CODE"""
    _names = ('CRC-8/I-CODE',)
    _width = 8
    _poly = 0x1d
    _initvalue = 0xfd
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x7e
    _residue = 0x00


class Crc8Lte(Crc8Base):
    """CRC-8/LTE"""
    _names = ('CRC-8/LTE',)
    _width = 8
    _poly = 0x9b
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0xea
    _residue = 0x00


class Crc8MaximDow(Crc8Base):
    """CRC-8/MAXIM-DOW

    Aliases: CRC-8/MAXIM, DOW-CRC
    """
    _names = ('CRC-8/MAXIM-DOW', 'CRC-8/MAXIM', 'DOW-CRC')
    _width = 8
    _poly = 0x31
    _initvalue = 0x00
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0xa1
    _residue = 0x00


Crc8Maxim = Crc8MaximDow
CrcDow = Crc8MaximDow


class Crc8MifareMad(Crc8Base):
    """CRC-8/MIFARE-MAD"""
    _names = ('CRC-8/MIFARE-MAD',)
    _width = 8
    _poly = 0x1d
    _initvalue = 0xc7
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x99
    _residue = 0x00


class Crc8Nrsc5(Crc8Base):
    """CRC-8/NRSC-5"""
    _names = ('CRC-8/NRSC-5',)
    _width = 8
    _poly = 0x31
    _initvalue = 0xff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0xf7
    _residue = 0x00


class Crc8Opensafety(Crc8Base):
    """CRC-8/OPENSAFETY"""
    _names = ('CRC-8/OPENSAFETY',)
    _width = 8
    _poly = 0x2f
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0x3e
    _residue = 0x00


class Crc8Rohc(Crc8Base):
    """CRC-8/ROHC"""
    _names = ('CRC-8/ROHC',)
    _width = 8
    _poly = 0x07
    _initvalue = 0xff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0xd0
    _residue = 0x00


class Crc8SaeJ1850(Crc8Base):
    """CRC-8/SAE-J1850"""
    _names = ('CRC-8/SAE-J1850',)
    _width = 8
    _poly = 0x1d
    _initvalue = 0xff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xff
    _check_result = 0x4b
    _residue = 0xc4


class Crc8Smbus(Crc8Base):
    """CRC-8/SMBUS

    Aliases: CRC-8
    """
    _names = ('CRC-8/SMBUS', 'CRC-8')
    _width = 8
    _poly = 0x07
    _initvalue = 0x00
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00
    _check_result = 0xf4
    _residue = 0x00


Crc8 = Crc8Smbus


class Crc8Tech3250(Crc8Base):
    """CRC-8/TECH-3250

    Aliases: CRC-8/AES, CRC-8/EBU
    """
    _names = ('CRC-8/TECH-3250', 'CRC-8/AES', 'CRC-8/EBU')
    _width = 8
    _poly = 0x1d
    _initvalue = 0xff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x97
    _residue = 0x00


Crc8Aes = Crc8Tech3250
Crc8Ebu = Crc8Tech3250


class Crc8Wcdma(Crc8Base):
    """CRC-8/WCDMA"""
    _names = ('CRC-8/WCDMA',)
    _width = 8
    _poly = 0x9b
    _initvalue = 0x00
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00
    _check_result = 0x25
    _residue = 0x00


class Crc10Atm(CrcBase):
    """CRC-10/ATM

    Aliases: CRC-10, CRC-10/I-610
    """
    _names = ('CRC-10/ATM', 'CRC-10', 'CRC-10/I-610')
    _width = 10
    _poly = 0x233
    _initvalue = 0x000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000
    _check_result = 0x199
    _residue = 0x000


Crc10 = Crc10Atm
Crc10I610 = Crc10Atm


class Crc10Cdma2000(CrcBase):
    """CRC-10/CDMA2000"""
    _names = ('CRC-10/CDMA2000',)
    _width = 10
    _poly = 0x3d9
    _initvalue = 0x3ff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000
    _check_result = 0x233
    _residue = 0x000


class Crc10Gsm(CrcBase):
    """CRC-10/GSM"""
    _names = ('CRC-10/GSM',)
    _width = 10
    _poly = 0x175
    _initvalue = 0x000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x3ff
    _check_result = 0x12a
    _residue = 0x0c6


class Crc11Flexray(CrcBase):
    """CRC-11/FLEXRAY

    Aliases: CRC-11
    """
    _names = ('CRC-11/FLEXRAY', 'CRC-11')
    _width = 11
    _poly = 0x385
    _initvalue = 0x01a
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000
    _check_result = 0x5a3
    _residue = 0x000


Crc11 = Crc11Flexray


class Crc11Umts(CrcBase):
    """CRC-11/UMTS"""
    _names = ('CRC-11/UMTS',)
    _width = 11
    _poly = 0x307
    _initvalue = 0x000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000
    _check_result = 0x061
    _residue = 0x000


class Crc12Cdma2000(CrcBase):
    """CRC-12/CDMA2000"""
    _names = ('CRC-12/CDMA2000',)
    _width = 12
    _poly = 0xf13
    _initvalue = 0xfff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000
    _check_result = 0xd4d
    _residue = 0x000


class Crc12Dect(CrcBase):
    """CRC-12/DECT

    Aliases: CRC-12-X
    """
    _names = ('CRC-12/DECT', 'CRC-12-X')
    _width = 12
    _poly = 0x80f
    _initvalue = 0x000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000
    _check_result = 0xf5b
    _residue = 0x000


Crc12X = Crc12Dect


class Crc12Gsm(CrcBase):
    """CRC-12/GSM"""
    _names = ('CRC-12/GSM',)
    _width = 12
    _poly = 0xd31
    _initvalue = 0x000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xfff
    _check_result = 0xb34
    _residue = 0x178


class Crc12Umts(CrcBase):
    """CRC-12/UMTS

    Aliases: CRC-12/3GPP
    """
    _names = ('CRC-12/UMTS', 'CRC-12/3GPP')
    _width = 12
    _poly = 0x80f
    _initvalue = 0x000
    _reflect_input = False
    _reflect_output = True
    _xor_output = 0x000
    _check_result = 0xdaf
    _residue = 0x000


Crc123Gpp = Crc12Umts


class Crc13Bbc(CrcBase):
    """CRC-13/BBC"""
    _names = ('CRC-13/BBC',)
    _width = 13
    _poly = 0x1cf5
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x04fa
    _residue = 0x0000


class Crc14Darc(CrcBase):
    """CRC-14/DARC"""
    _names = ('CRC-14/DARC',)
    _width = 14
    _poly = 0x0805
    _initvalue = 0x0000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0x082d
    _residue = 0x0000


class Crc14Gsm(CrcBase):
    """CRC-14/GSM"""
    _names = ('CRC-14/GSM',)
    _width = 14
    _poly = 0x202d
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x3fff
    _check_result = 0x30ae
    _residue = 0x031e


class Crc15Can(CrcBase):
    """CRC-15/CAN

    Aliases: CRC-15
    """
    _names = ('CRC-15/CAN', 'CRC-15')
    _width = 15
    _poly = 0x4599
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x059e
    _residue = 0x0000


Crc15 = Crc15Can


class Crc15Mpt1327(CrcBase):
    """CRC-15/MPT1327"""
    _names = ('CRC-15/MPT1327',)
    _width = 15
    _poly = 0x6815
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0001
    _check_result = 0x2566
    _residue = 0x6815


class Crc16Arc(Crc16Base):
    """CRC-16/ARC

    Aliases: ARC, CRC-16/LHA, CRC-IBM
    """
    _names = ('CRC-16/ARC', 'ARC', 'CRC-16/LHA', 'CRC-IBM')
    _width = 16
    _poly = 0x8005
    _initvalue = 0x0000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0xbb3d
    _residue = 0x0000


CrcArc = Crc16Arc
Crc16Lha = Crc16Arc
CrcIbm = Crc16Arc


class Crc16Cdma2000(Crc16Base):
    """CRC-16/CDMA2000"""
    _names = ('CRC-16/CDMA2000',)
    _width = 16
    _poly = 0xc867
    _initvalue = 0xffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x4c06
    _residue = 0x0000


class Crc16Cms(Crc16Base):
    """CRC-16/CMS"""
    _names = ('CRC-16/CMS',)
    _width = 16
    _poly = 0x8005
    _initvalue = 0xffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0xaee7
    _residue = 0x0000


class Crc16Dds110(Crc16Base):
    """CRC-16/DDS-110"""
    _names = ('CRC-16/DDS-110',)
    _width = 16
    _poly = 0x8005
    _initvalue = 0x800d
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x9ecf
    _residue = 0x0000


class Crc16DectR(Crc16Base):
    """CRC-16/DECT-R

    Aliases: R-CRC-16
    """
    _names = ('CRC-16/DECT-R', 'R-CRC-16')
    _width = 16
    _poly = 0x0589
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0001
    _check_result = 0x007e
    _residue = 0x0589


Crc16R = Crc16DectR


class Crc16DectX(Crc16Base):
    """CRC-16/DECT-X

    Aliases: X-CRC-16
    """
    _names = ('CRC-16/DECT-X', 'X-CRC-16')
    _width = 16
    _poly = 0x0589
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x007f
    _residue = 0x0000


Crc16X = Crc16DectX


class Crc16Dnp(Crc16Base):
    """CRC-16/DNP"""
    _names = ('CRC-16/DNP',)
    _width = 16
    _poly = 0x3d65
    _initvalue = 0x0000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffff
    _check_result = 0xea82
    _residue = 0x66c5


class Crc16En13757(Crc16Base):
    """CRC-16/EN-13757"""
    _names = ('CRC-16/EN-13757',)
    _width = 16
    _poly = 0x3d65
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffff
    _check_result = 0xc2b7
    _residue = 0xa366


class Crc16Genibus(Crc16Base):
    """CRC-16/GENIBUS

    Aliases: CRC-16/DARC, CRC-16/EPC, CRC-16/EPC-C1G2, CRC-16/I-CODE
    """
    _names = ('CRC-16/GENIBUS', 'CRC-16/DARC', 'CRC-16/EPC', 'CRC-16/EPC-C1G2', 'CRC-16/I-CODE')
    _width = 16
    _poly = 0x1021
    _initvalue = 0xffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffff
    _check_result = 0xd64e
    _residue = 0x1d0f


Crc16Darc = Crc16Genibus
Crc16Epc = Crc16Genibus
Crc16EpcC1G2 = Crc16Genibus
Crc16ICode = Crc16Genibus


class Crc16Gsm(Crc16Base):
    """CRC-16/GSM"""
    _names = ('CRC-16/GSM',)
    _width = 16
    _poly = 0x1021
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffff
    _check_result = 0xce3c
    _residue = 0x1d0f


class Crc16Ibm3740(Crc16Base):
    """CRC-16/IBM-3740

    Aliases: CRC-16/AUTOSAR, CRC-16/CCITT-FALSE
    """
    _names = ('CRC-16/IBM-3740', 'CRC-16/AUTOSAR', 'CRC-16/CCITT-FALSE')
    _width = 16
    _poly = 0x1021
    _initvalue = 0xffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x29b1
    _residue = 0x0000


Crc16Autosar = Crc16Ibm3740
Crc16CcittFalse = Crc16Ibm3740


class Crc16IbmSdlc(Crc16Base):
    """CRC-16/IBM-SDLC

    Aliases: CRC-16/ISO-HDLC, CRC-16/ISO-IEC-14443-3-B, CRC-16/X-25, CRC-B, X-25
    """
    _names = ('CRC-16/IBM-SDLC', 'CRC-16/ISO-HDLC', 'CRC-16/ISO-IEC-14443-3-B', 'CRC-16/X-25', 'CRC-B', 'X-25')
    _width = 16
    _poly = 0x1021
    _initvalue = 0xffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffff
    _check_result = 0x906e
    _residue = 0xf0b8


Crc16IsoHdlc = Crc16IbmSdlc
Crc16IsoIec144433B = Crc16IbmSdlc
Crc16X25 = Crc16IbmSdlc
CrcB = Crc16IbmSdlc
CrcX25 = Crc16IbmSdlc


class Crc16IsoIec144433A(Crc16Base):
    """CRC-16/ISO-IEC-14443-3-A

    Aliases: CRC-A
    """
    _names = ('CRC-16/ISO-IEC-14443-3-A', 'CRC-A')
    _width = 16
    _poly = 0x1021
    _initvalue = 0xc6c6
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0xbf05
    _residue = 0x0000


CrcA = Crc16IsoIec144433A


class Crc16Kermit(Crc16Base):
    """CRC-16/KERMIT

    Aliases: CRC-16/CCITT, CRC-16/CCITT-TRUE, CRC-16/V-41-LSB, CRC-CCITT, KERMIT
    """
    _names = ('CRC-16/KERMIT', 'CRC-16/CCITT', 'CRC-16/CCITT-TRUE', 'CRC-16/V-41-LSB', 'CRC-CCITT', 'KERMIT')
    _width = 16
    _poly = 0x1021
    _initvalue = 0x0000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0x2189
    _residue = 0x0000


Crc16Ccitt = Crc16Kermit
Crc16CcittTrue = Crc16Kermit
Crc16V41Lsb = Crc16Kermit
CrcCcitt = Crc16Kermit
CrcKermit = Crc16Kermit


class Crc16Lj1200(Crc16Base):
    """CRC-16/LJ1200"""
    _names = ('CRC-16/LJ1200',)
    _width = 16
    _poly = 0x6f63
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0xbdf4
    _residue = 0x0000


class Crc16M17(Crc16Base):
    """CRC-16/M17"""
    _names = ('CRC-16/M17',)
    _width = 16
    _poly = 0x5935
    _initvalue = 0xffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x772b
    _residue = 0x0000


class Crc16MaximDow(Crc16Base):
    """CRC-16/MAXIM-DOW

    Aliases: CRC-16/MAXIM
    """
    _names = ('CRC-16/MAXIM-DOW', 'CRC-16/MAXIM')
    _width = 16
    _poly = 0x8005
    _initvalue = 0x0000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffff
    _check_result = 0x44c2
    _residue = 0xb001


Crc16Maxim = Crc16MaximDow


class Crc16Mcrf4Xx(Crc16Base):
    """CRC-16/MCRF4XX"""
    _names = ('CRC-16/MCRF4XX',)
    _width = 16
    _poly = 0x1021
    _initvalue = 0xffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0x6f91
    _residue = 0x0000


Crc16Mcrf4XX = Crc16Mcrf4Xx
Crcc16Mcrf4xx = Crc16Mcrf4Xx


class Crc16Modbus(Crc16Base):
    """CRC-16/MODBUS

    Aliases: MODBUS
    """
    _names = ('CRC-16/MODBUS', 'MODBUS')
    _width = 16
    _poly = 0x8005
    _initvalue = 0xffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0x4b37
    _residue = 0x0000


CrcModbus = Crc16Modbus


class Crc16Nrsc5(Crc16Base):
    """CRC-16/NRSC-5"""
    _names = ('CRC-16/NRSC-5',)
    _width = 16
    _poly = 0x080b
    _initvalue = 0xffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0xa066
    _residue = 0x0000


class Crc16OpensafetyA(Crc16Base):
    """CRC-16/OPENSAFETY-A"""
    _names = ('CRC-16/OPENSAFETY-A',)
    _width = 16
    _poly = 0x5935
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x5d38
    _residue = 0x0000


class Crc16OpensafetyB(Crc16Base):
    """CRC-16/OPENSAFETY-B"""
    _names = ('CRC-16/OPENSAFETY-B',)
    _width = 16
    _poly = 0x755b
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x20fe
    _residue = 0x0000


class Crc16Profibus(Crc16Base):
    """CRC-16/PROFIBUS

    Aliases: CRC-16/IEC-61158-2
    """
    _names = ('CRC-16/PROFIBUS', 'CRC-16/IEC-61158-2')
    _width = 16
    _poly = 0x1dcf
    _initvalue = 0xffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffff
    _check_result = 0xa819
    _residue = 0xe394


Crc16Iec611582 = Crc16Profibus


class Crc16Riello(Crc16Base):
    """CRC-16/RIELLO"""
    _names = ('CRC-16/RIELLO',)
    _width = 16
    _poly = 0x1021
    _initvalue = 0xb2aa
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0x63d0
    _residue = 0x0000


class Crc16SpiFujitsu(Crc16Base):
    """CRC-16/SPI-FUJITSU

    Aliases: CRC-16/AUG-CCITT
    """
    _names = ('CRC-16/SPI-FUJITSU', 'CRC-16/AUG-CCITT')
    _width = 16
    _poly = 0x1021
    _initvalue = 0x1d0f
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0xe5cc
    _residue = 0x0000


Crc16AugCcitt = Crc16SpiFujitsu


class Crc16T10Dif(Crc16Base):
    """CRC-16/T10-DIF"""
    _names = ('CRC-16/T10-DIF',)
    _width = 16
    _poly = 0x8bb7
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0xd0db
    _residue = 0x0000


class Crc16Teledisk(Crc16Base):
    """CRC-16/TELEDISK"""
    _names = ('CRC-16/TELEDISK',)
    _width = 16
    _poly = 0xa097
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x0fb3
    _residue = 0x0000


class Crc16Tms37157(Crc16Base):
    """CRC-16/TMS37157"""
    _names = ('CRC-16/TMS37157',)
    _width = 16
    _poly = 0x1021
    _initvalue = 0x89ec
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000
    _check_result = 0x26b1
    _residue = 0x0000


class Crc16Umts(Crc16Base):
    """CRC-16/UMTS

    Aliases: CRC-16/BUYPASS, CRC-16/VERIFONE
    """
    _names = ('CRC-16/UMTS', 'CRC-16/BUYPASS', 'CRC-16/VERIFONE')
    _width = 16
    _poly = 0x8005
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0xfee8
    _residue = 0x0000


Crc16Buypass = Crc16Umts
Crc16Verifone = Crc16Umts


class Crc16Usb(Crc16Base):
    """CRC-16/USB"""
    _names = ('CRC-16/USB',)
    _width = 16
    _poly = 0x8005
    _initvalue = 0xffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffff
    _check_result = 0xb4c8
    _residue = 0xb001


class Crc16Xmodem(Crc16Base):
    """CRC-16/XMODEM

    Aliases: CRC-16/ACORN, CRC-16/LTE, CRC-16/V-41-MSB, XMODEM, ZMODEM
    """
    _names = ('CRC-16/XMODEM', 'CRC-16/ACORN', 'CRC-16/LTE', 'CRC-16/V-41-MSB', 'XMODEM', 'ZMODEM')
    _width = 16
    _poly = 0x1021
    _initvalue = 0x0000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000
    _check_result = 0x31c3
    _residue = 0x0000


Crc16Acorn = Crc16Xmodem
Crc16Lte = Crc16Xmodem
Crc16V41Msb = Crc16Xmodem
CrcXmodem = Crc16Xmodem
CrcZmodem = Crc16Xmodem
Crc16 = Crc16Xmodem


class Crc17CanFd(CrcBase):
    """CRC-17/CAN-FD"""
    _names = ('CRC-17/CAN-FD',)
    _width = 17
    _poly = 0x1685b
    _initvalue = 0x00000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00000
    _check_result = 0x04f03
    _residue = 0x00000


class Crc21CanFd(CrcBase):
    """CRC-21/CAN-FD"""
    _names = ('CRC-21/CAN-FD',)
    _width = 21
    _poly = 0x102899
    _initvalue = 0x000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000000
    _check_result = 0x0ed841
    _residue = 0x000000


class Crc24Ble(CrcBase):
    """CRC-24/BLE"""
    _names = ('CRC-24/BLE',)
    _width = 24
    _poly = 0x00065b
    _initvalue = 0x555555
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x000000
    _check_result = 0xc25a56
    _residue = 0x000000


class Crc24FlexrayA(CrcBase):
    """CRC-24/FLEXRAY-A"""
    _names = ('CRC-24/FLEXRAY-A',)
    _width = 24
    _poly = 0x5d6dcb
    _initvalue = 0xfedcba
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000000
    _check_result = 0x7979bd
    _residue = 0x000000


class Crc24FlexrayB(CrcBase):
    """CRC-24/FLEXRAY-B"""
    _names = ('CRC-24/FLEXRAY-B',)
    _width = 24
    _poly = 0x5d6dcb
    _initvalue = 0xabcdef
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000000
    _check_result = 0x1f23b8
    _residue = 0x000000


class Crc24Interlaken(CrcBase):
    """CRC-24/INTERLAKEN"""
    _names = ('CRC-24/INTERLAKEN',)
    _width = 24
    _poly = 0x328b63
    _initvalue = 0xffffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffffff
    _check_result = 0xb4f3e6
    _residue = 0x144e63


class Crc24LteA(CrcBase):
    """CRC-24/LTE-A"""
    _names = ('CRC-24/LTE-A',)
    _width = 24
    _poly = 0x864cfb
    _initvalue = 0x000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000000
    _check_result = 0xcde703
    _residue = 0x000000


class Crc24LteB(CrcBase):
    """CRC-24/LTE-B"""
    _names = ('CRC-24/LTE-B',)
    _width = 24
    _poly = 0x800063
    _initvalue = 0x000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000000
    _check_result = 0x23ef52
    _residue = 0x000000


class Crc24Openpgp(CrcBase):
    """CRC-24/OPENPGP

    Aliases: CRC-24
    """
    _names = ('CRC-24/OPENPGP', 'CRC-24')
    _width = 24
    _poly = 0x864cfb
    _initvalue = 0xb704ce
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x000000
    _check_result = 0x21cf02
    _residue = 0x000000


Crc24 = Crc24Openpgp
Crc24OpenPgp = Crc24Openpgp


class Crc24Os9(CrcBase):
    """CRC-24/OS-9"""
    _names = ('CRC-24/OS-9',)
    _width = 24
    _poly = 0x800063
    _initvalue = 0xffffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffffff
    _check_result = 0x200fa5
    _residue = 0x800fe3


class Crc30Cdma(CrcBase):
    """CRC-30/CDMA"""
    _names = ('CRC-30/CDMA',)
    _width = 30
    _poly = 0x2030b9c7
    _initvalue = 0x3fffffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x3fffffff
    _check_result = 0x04c34abf
    _residue = 0x34efa55a


class Crc31Philips(CrcBase):
    """CRC-31/PHILIPS"""
    _names = ('CRC-31/PHILIPS',)
    _width = 31
    _poly = 0x04c11db7
    _initvalue = 0x7fffffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x7fffffff
    _check_result = 0x0ce9e46c
    _residue = 0x4eaf26f1


class Crc32Aixm(Crc32Base):
    """CRC-32/AIXM

    Aliases: CRC-32Q
    """
    _names = ('CRC-32/AIXM', 'CRC-32Q')
    _width = 32
    _poly = 0x814141ab
    _initvalue = 0x00000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00000000
    _check_result = 0x3010bf7f
    _residue = 0x00000000


Crc32Q = Crc32Aixm
Crc32q = Crc32Aixm


class Crc32Autosar(Crc32Base):
    """CRC-32/AUTOSAR"""
    _names = ('CRC-32/AUTOSAR',)
    _width = 32
    _poly = 0xf4acfb13
    _initvalue = 0xffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffffffff
    _check_result = 0x1697d06a
    _residue = 0x904cddbf


class Crc32Base91D(Crc32Base):
    """CRC-32/BASE91-D

    Aliases: CRC-32D
    """
    _names = ('CRC-32/BASE91-D', 'CRC-32D')
    _width = 32
    _poly = 0xa833982b
    _initvalue = 0xffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffffffff
    _check_result = 0x87315576
    _residue = 0x45270551


Crc32D = Crc32Base91D
Crc32d = Crc32Base91D


class Crc32Bzip2(Crc32Base):
    """CRC-32/BZIP2

    Aliases: CRC-32/AAL5, CRC-32/DECT-B, B-CRC-32
    """
    _names = ('CRC-32/BZIP2', 'CRC-32/AAL5', 'CRC-32/DECT-B', 'B-CRC-32')
    _width = 32
    _poly = 0x04c11db7
    _initvalue = 0xffffffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffffffff
    _check_result = 0xfc891918
    _residue = 0xc704dd7b


Crc32Aal5 = Crc32Bzip2
Crc32DectB = Crc32Bzip2
Crc32B = Crc32Bzip2


class Crc32CdRomEdc(Crc32Base):
    """CRC-32/CD-ROM-EDC"""
    _names = ('CRC-32/CD-ROM-EDC',)
    _width = 32
    _poly = 0x8001801b
    _initvalue = 0x00000000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00000000
    _check_result = 0x6ec2edc4
    _residue = 0x00000000


class Crc32Cksum(Crc32Base):
    """CRC-32/CKSUM

    Aliases: CKSUM, CRC-32/POSIX
    """
    _names = ('CRC-32/CKSUM', 'CKSUM', 'CRC-32/POSIX')
    _width = 32
    _poly = 0x04c11db7
    _initvalue = 0x00000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffffffff
    _check_result = 0x765e7680
    _residue = 0xc704dd7b


CrcCksum = Crc32Cksum
Crc32Posix = Crc32Cksum


class Crc32Iscsi(Crc32Base):
    """CRC-32/ISCSI

    Aliases: CRC-32/BASE91-C, CRC-32/CASTAGNOLI, CRC-32/INTERLAKEN, CRC-32C
    """
    _names = ('CRC-32/ISCSI', 'CRC-32/BASE91-C', 'CRC-32/CASTAGNOLI', 'CRC-32/INTERLAKEN', 'CRC-32C')
    _width = 32
    _poly = 0x1edc6f41
    _initvalue = 0xffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffffffff
    _check_result = 0xe3069283
    _residue = 0xb798b438


Crc32Base91C = Crc32Iscsi
Crc32Castagnoli = Crc32Iscsi
Crc32Interlaken = Crc32Iscsi
Crc32C = Crc32Iscsi
Crc32c = Crc32Iscsi


class Crc32IsoHdlc(Crc32Base):
    """CRC-32/ISO-HDLC

    Aliases: CRC-32, CRC-32/ADCCP, CRC-32/V-42, CRC-32/XZ, PKZIP
    """
    _names = ('CRC-32/ISO-HDLC', 'CRC-32', 'CRC-32/ADCCP', 'CRC-32/V-42', 'CRC-32/XZ', 'PKZIP')
    _width = 32
    _poly = 0x04c11db7
    _initvalue = 0xffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffffffff
    _check_result = 0xcbf43926
    _residue = 0xdebb20e3


Crc32 = Crc32IsoHdlc
Crc32Adccp = Crc32IsoHdlc
Crc32V42 = Crc32IsoHdlc
Crc32Xz = Crc32IsoHdlc
CrcPkzip = Crc32IsoHdlc


class Crc32Jamcrc(Crc32Base):
    """CRC-32/JAMCRC

    Aliases: JAMCRC
    """
    _names = ('CRC-32/JAMCRC', 'JAMCRC')
    _width = 32
    _poly = 0x04c11db7
    _initvalue = 0xffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00000000
    _check_result = 0x340bc6d9
    _residue = 0x00000000


CrcJamcrc = Crc32Jamcrc


class Crc32Mef(Crc32Base):
    """CRC-32/MEF"""
    _names = ('CRC-32/MEF',)
    _width = 32
    _poly = 0x741b8cd7
    _initvalue = 0xffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x00000000
    _check_result = 0xd2c22f51
    _residue = 0x00000000


class Crc32Mpeg2(Crc32Base):
    """CRC-32/MPEG-2"""
    _names = ('CRC-32/MPEG-2',)
    _width = 32
    _poly = 0x04c11db7
    _initvalue = 0xffffffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00000000
    _check_result = 0x0376e6e7
    _residue = 0x00000000


class Crc32Xfer(Crc32Base):
    """CRC-32/XFER

    Aliases: XFER
    """
    _names = ('CRC-32/XFER', 'XFER')
    _width = 32
    _poly = 0x000000af
    _initvalue = 0x00000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x00000000
    _check_result = 0xbd0be338
    _residue = 0x00000000


CrcXfer = Crc32Xfer


class Crc40Gsm(CrcBase):
    """CRC-40/GSM"""
    _names = ('CRC-40/GSM',)
    _width = 40
    _poly = 0x0004820009
    _initvalue = 0x0000000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffffffffff
    _check_result = 0xd4164fc646
    _residue = 0xc4ff8071ff


class Crc64Ecma182(CrcBase):
    """CRC-64/ECMA-182

    Aliases: CRC-64
    """
    _names = ('CRC-64/ECMA-182', 'CRC-64')
    _width = 64
    _poly = 0x42f0e1eba9ea3693
    _initvalue = 0x0000000000000000
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0x0000000000000000
    _check_result = 0x6c40df5f0b497347
    _residue = 0x0000000000000000


Crc64 = Crc64Ecma182


class Crc64GoIso(CrcBase):
    """CRC-64/GO-ISO"""
    _names = ('CRC-64/GO-ISO',)
    _width = 64
    _poly = 0x000000000000001b
    _initvalue = 0xffffffffffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffffffffffffffff
    _check_result = 0xb90956c775a41001
    _residue = 0x5300000000000000


class Crc64Ms(CrcBase):
    """CRC-64/MS"""
    _names = ('CRC-64/MS',)
    _width = 64
    _poly = 0x259c84cba6426349
    _initvalue = 0xffffffffffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000000000000000
    _check_result = 0x75d4b74f024eceea
    _residue = 0x0000000000000000


class Crc64Nvme(CrcBase):
    """CRC-64/NVME"""
    _names = ('CRC-64/NVME',)
    _width = 64
    _poly = 0xad93d23594c93659
    _initvalue = 0xffffffffffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffffffffffffffff
    _check_result = 0xae8b14860a799888
    _residue = 0xf310303b2b6f6e42


class Crc64Redis(CrcBase):
    """CRC-64/REDIS"""
    _names = ('CRC-64/REDIS',)
    _width = 64
    _poly = 0xad93d23594c935a9
    _initvalue = 0x0000000000000000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x0000000000000000
    _check_result = 0xe9c6d914c4b8d9ca
    _residue = 0x0000000000000000


class Crc64We(CrcBase):
    """CRC-64/WE"""
    _names = ('CRC-64/WE',)
    _width = 64
    _poly = 0x42f0e1eba9ea3693
    _initvalue = 0xffffffffffffffff
    _reflect_input = False
    _reflect_output = False
    _xor_output = 0xffffffffffffffff
    _check_result = 0x62ec59e3f1a4f00a
    _residue = 0xfcacbebd5931a992


class Crc64Xz(CrcBase):
    """CRC-64/XZ

    Aliases: CRC-64/GO-ECMA
    """
    _names = ('CRC-64/XZ', 'CRC-64/GO-ECMA')
    _width = 64
    _poly = 0x42f0e1eba9ea3693
    _initvalue = 0xffffffffffffffff
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0xffffffffffffffff
    _check_result = 0x995dc9bbdf1939fa
    _residue = 0x49958c9abd7d353f


Crc64GoEcma = Crc64Xz


class Crc82Darc(CrcBase):
    """CRC-82/DARC"""
    _names = ('CRC-82/DARC',)
    _width = 82
    _poly = 0x0308c0111011401440411
    _initvalue = 0x000000000000000000000
    _reflect_input = True
    _reflect_output = True
    _xor_output = 0x000000000000000000000
    _check_result = 0x09ea83f625023801fd612
    _residue = 0x000000000000000000000


ALLCRCCLASSES = (
    Crc3Gsm, Crc3Rohc, Crc4G704, Crc4Interlaken, Crc5EpcC1G2, Crc5G704, Crc5Usb, Crc6Cdma2000A, Crc6Cdma2000B,
    Crc6Darc, Crc6G704, Crc6Gsm, Crc7Mmc, Crc7Rohc, Crc7Umts, Crc8Autosar, Crc8Bluetooth, Crc8Cdma2000, Crc8Darc,
    Crc8DvbS2, Crc8GsmA, Crc8GsmB, Crc8Hitag, Crc8I4321, Crc8ICode, Crc8Lte, Crc8MaximDow, Crc8MifareMad, Crc8Nrsc5,
    Crc8Opensafety, Crc8Rohc, Crc8SaeJ1850, Crc8Smbus, Crc8Tech3250, Crc8Wcdma, Crc10Atm, Crc10Cdma2000, Crc10Gsm,
    Crc11Flexray, Crc11Umts, Crc12Cdma2000, Crc12Dect, Crc12Gsm, Crc12Umts, Crc13Bbc, Crc14Darc, Crc14Gsm, Crc15Can,
    Crc15Mpt1327, Crc16Arc, Crc16Cdma2000, Crc16Cms, Crc16Dds110, Crc16DectR, Crc16DectX, Crc16Dnp, Crc16En13757,
    Crc16Genibus, Crc16Gsm, Crc16Ibm3740, Crc16IbmSdlc, Crc16IsoIec144433A, Crc16Kermit, Crc16Lj1200, Crc16M17,
    Crc16MaximDow, Crc16Mcrf4Xx, Crc16Modbus, Crc16Nrsc5, Crc16OpensafetyA, Crc16OpensafetyB, Crc16Profibus,
    Crc16Riello, Crc16SpiFujitsu, Crc16T10Dif, Crc16Teledisk, Crc16Tms37157, Crc16Umts, Crc16Usb, Crc16Xmodem,
    Crc17CanFd, Crc21CanFd, Crc24Ble, Crc24FlexrayA, Crc24FlexrayB, Crc24Interlaken, Crc24LteA, Crc24LteB,
    Crc24Openpgp, Crc24Os9, Crc30Cdma, Crc31Philips, Crc32Aixm, Crc32Autosar, Crc32Base91D, Crc32Bzip2, Crc32CdRomEdc,
    Crc32Cksum, Crc32Iscsi, Crc32IsoHdlc, Crc32Jamcrc, Crc32Mef, Crc32Mpeg2, Crc32Xfer, Crc40Gsm, Crc64Ecma182,
    Crc64GoIso, Crc64Ms, Crc64Nvme, Crc64Redis, Crc64We, Crc64Xz, Crc82Darc
)

ALLCRCCLASSES_ALIASES = (
    Crc3Gsm, Crc3Rohc, Crc4G704, Crc4Itu, Crc4Interlaken, Crc5EpcC1G2, Crc5Epc, Crc5G704, Crc5Itu, Crc5Usb,
    Crc6Cdma2000A, Crc6Cdma2000B, Crc6Darc, Crc6G704, Crc6Itu, Crc6Gsm, Crc7Mmc, Crc7, Crc7Rohc, Crc7Umts, Crc8Autosar,
    Crc8Bluetooth, Crc8Cdma2000, Crc8Darc, Crc8DvbS2, Crc8GsmA, Crc8GsmB, Crc8Hitag, Crc8I4321, Crc8Itu, Crc8ICode,
    Crc8Lte, Crc8MaximDow, Crc8Maxim, CrcDow, Crc8MifareMad, Crc8Nrsc5, Crc8Opensafety, Crc8Rohc, Crc8SaeJ1850,
    Crc8Smbus, Crc8, Crc8Tech3250, Crc8Aes, Crc8Ebu, Crc8Wcdma, Crc10Atm, Crc10, Crc10I610, Crc10Cdma2000, Crc10Gsm,
    Crc11Flexray, Crc11, Crc11Umts, Crc12Cdma2000, Crc12Dect, Crc12X, Crc12Gsm, Crc12Umts, Crc123Gpp, Crc13Bbc,
    Crc14Darc, Crc14Gsm, Crc15Can, Crc15, Crc15Mpt1327, Crc16Arc, CrcArc, Crc16Lha, CrcIbm, Crc16Cdma2000, Crc16Cms,
    Crc16Dds110, Crc16DectR, Crc16R, Crc16DectX, Crc16X, Crc16Dnp, Crc16En13757, Crc16Genibus, Crc16Darc, Crc16Epc,
    Crc16EpcC1G2, Crc16ICode, Crc16Gsm, Crc16Ibm3740, Crc16Autosar, Crc16CcittFalse, Crc16IbmSdlc, Crc16IsoHdlc,
    Crc16IsoIec144433B, Crc16X25, CrcB, CrcX25, Crc16IsoIec144433A, CrcA, Crc16Kermit, Crc16Ccitt, Crc16CcittTrue,
    Crc16V41Lsb, CrcCcitt, CrcKermit, Crc16Lj1200, Crc16M17, Crc16MaximDow, Crc16Maxim, Crc16Mcrf4Xx, Crc16Mcrf4XX,
    Crcc16Mcrf4xx, Crc16Modbus, CrcModbus, Crc16Nrsc5, Crc16OpensafetyA, Crc16OpensafetyB, Crc16Profibus,
    Crc16Iec611582, Crc16Riello, Crc16SpiFujitsu, Crc16AugCcitt, Crc16T10Dif, Crc16Teledisk, Crc16Tms37157, Crc16Umts,
    Crc16Buypass, Crc16Verifone, Crc16Usb, Crc16Xmodem, Crc16Acorn, Crc16Lte, Crc16V41Msb, CrcXmodem, CrcZmodem, Crc16,
    Crc17CanFd, Crc21CanFd, Crc24Ble, Crc24FlexrayA, Crc24FlexrayB, Crc24Interlaken, Crc24LteA, Crc24LteB,
    Crc24Openpgp, Crc24, Crc24OpenPgp, Crc24Os9, Crc30Cdma, Crc31Philips, Crc32Aixm, Crc32Q, Crc32q, Crc32Autosar,
    Crc32Base91D, Crc32D, Crc32d, Crc32Bzip2, Crc32Aal5, Crc32DectB, Crc32B, Crc32CdRomEdc, Crc32Cksum, CrcCksum,
    Crc32Posix, Crc32Iscsi, Crc32Base91C, Crc32Castagnoli, Crc32Interlaken, Crc32C, Crc32c, Crc32IsoHdlc, Crc32,
    Crc32Adccp, Crc32V42, Crc32Xz, CrcPkzip, Crc32Jamcrc, CrcJamcrc, Crc32Mef, Crc32Mpeg2, Crc32Xfer, CrcXfer,
    Crc40Gsm, Crc64Ecma182, Crc64, Crc64GoIso, Crc64Ms, Crc64Nvme, Crc64Redis, Crc64We, Crc64Xz, Crc64GoEcma, Crc82Darc
)
