""" userattribute.py
"""
import struct

from .types import UserAttribute

from ...constants import ImageEncoding

from ...decorators import sdproperty


__all__ = ('Image',)


class Image(UserAttribute):
    """
    5.12.1. The Image Attribute Subpacket

    The Image Attribute subpacket is used to encode an image, presumably
    (but not required to be) that of the key owner.

    The Image Attribute subpacket begins with an image header.  The first
    two octets of the image header contain the length of the image
    header.  Note that unlike other multi-octet numerical values in this
    document, due to a historical accident this value is encoded as a
    little-endian number.  The image header length is followed by a
    single octet for the image header version.  The only currently
    defined version of the image header is 1, which is a 16-octet image
    header.  The first three octets of a version 1 image header are thus
    0x10, 0x00, 0x01.

    The fourth octet of a version 1 image header designates the encoding
    format of the image.  The only currently defined encoding format is
    the value 1 to indicate JPEG.  Image format types 100 through 110 are
    reserved for private or experimental use.  The rest of the version 1
    image header is made up of 12 reserved octets, all of which MUST be
    set to 0.

    The rest of the image subpacket contains the image itself.  As the
    only currently defined image type is JPEG, the image is encoded in
    the JPEG File Interchange Format (JFIF), a standard file format for
    JPEG images [JFIF].

    An implementation MAY try to determine the type of an image by
    examination of the image data if it is unable to handle a particular
    version of the image header or if a specified encoding format value
    is not recognized.
    """
    __typeid__ = 0x01

    @sdproperty
    def version(self):
        return self._version

    @version.register(int)
    def version_int(self, val):
        self._version = val

    @sdproperty
    def iencoding(self):
        return self._iencoding

    @iencoding.register(int)
    @iencoding.register(ImageEncoding)
    def iencoding_int(self, val):
        try:
            self._iencoding = ImageEncoding(val)

        except ValueError:  # pragma: no cover
            self._iencoding = val

    @sdproperty
    def image(self):
        return self._image

    @image.register(bytes)
    @image.register(bytearray)
    def image_bin(self, val):
        self._image = bytearray(val)

    def __init__(self):
        super(Image, self).__init__()
        self.version = 1
        self.iencoding = 1
        self.image = bytearray()

    def __bytearray__(self):
        _bytes = super(Image, self).__bytearray__()

        if self.version == 1:
            # v1 image header length is always 16 bytes
            # and stored little-endian due to an 'historical accident'
            _bytes += struct.pack('<hbbiii', 16, self.version, self.iencoding, 0, 0, 0)

        _bytes += self.image
        return _bytes

    def parse(self, packet):
        super(Image, self).parse(packet)

        with memoryview(packet) as _head:
            _, self.version, self.iencoding, _, _, _ = struct.unpack_from('<hbbiii', _head[:16].tobytes())
        del packet[:16]

        self.image = packet[:(self.header.length - 17)]
        del packet[:(self.header.length - 17)]
