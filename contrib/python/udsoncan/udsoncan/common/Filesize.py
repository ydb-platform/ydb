__all__ = ['Filesize']
import math

from typing import Optional


class Filesize:
    """
    This class represent a file size used by the RequestFileTransfer service.

    :param uncompressed: Represent the uncompressed size in bytes
    :type uncompressed: int

    :param compressed: Represent the compressed size in bytes
    :type compressed: int

    :param width: The number of byte that should be used to encode the filesize in a payload
    :type width: int
    """

    uncompressed: Optional[int]
    compressed: Optional[int]
    width: int

    def __init__(self, uncompressed: Optional[int] = None, compressed: Optional[int] = None, width: Optional[int] = None):

        if uncompressed is None and compressed is None:
            raise ValueError('At least one size must be specified')

        if uncompressed is not None:
            if not isinstance(uncompressed, int):
                raise ValueError('Uncompressed size must be an integer')

            if uncompressed < 0:
                raise ValueError("Uncompressed size must be an integer greater than 0")

        if compressed is not None:
            if not isinstance(compressed, int):
                raise ValueError('Compressed size must be an integer')

            if compressed < 0:
                raise ValueError("Comrpessed size must be an integer greater than 0")

        if width is not None:
            if not isinstance(width, int):
                raise ValueError('Width must be an integer')

            if width < 0:
                raise ValueError("Width size must be an integer greater than 0")
            maxsize = 2**(width * 8) - 1

            if compressed is not None:
                if compressed > maxsize:
                    raise ValueError("With width=%d, compressed size must be smaller than %d" % (width, maxsize))

            if uncompressed is not None:
                if uncompressed > maxsize:
                    raise ValueError("With width=%d, uncompressed size must be smaller than %d" % (width, maxsize))
        else:
            width_value = 0
            if uncompressed is not None:
                width_value = max(width_value, uncompressed)

            if compressed is not None:
                width_value = max(width_value, compressed)
            width = math.ceil(math.log2(width_value + 1) / 8)

        self.uncompressed = uncompressed
        self.compressed = compressed
        self.width = width

    def get_width(self) -> int:
        return self.width

    def get_uncompressed_bytes(self) -> bytes:
        if self.uncompressed is not None:
            return self.uncompressed.to_bytes(self.width, byteorder='big')
        else:
            return b''

    def get_compressed_bytes(self) -> bytes:
        if self.compressed is not None:
            return self.compressed.to_bytes(self.width, byteorder='big')
        else:
            return b''

    def __str__(self) -> str:
        uncompressed_str = 'None' if self.uncompressed is None else '0x%02x' % self.uncompressed
        compressed_str = 'None' if self.compressed is None else '0x%02x' % self.compressed
        width_str = 'None' if self.width is None else '0x%02x' % self.width

        return "Filesize<Uncompressed=%s, Compressed=%s. Width=%s>" % (uncompressed_str, compressed_str, width_str)

    def __repr__(self) -> str:
        uncompressed_str = 'None' if self.uncompressed is None else '0x%02x' % self.uncompressed
        compressed_str = 'None' if self.compressed is None else '0x%02x' % self.compressed
        width_str = 'None' if self.width is None else '0x%02x' % self.width

        return "<Filesize: Uncompressed=%s, Compressed=%s. Width=%s at 0x%08x>" % (uncompressed_str, compressed_str, width_str, id(self))
