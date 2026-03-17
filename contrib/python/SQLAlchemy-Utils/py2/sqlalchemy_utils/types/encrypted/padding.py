import six


class InvalidPaddingError(Exception):
    pass


class Padding(object):
    """Base class for padding and unpadding."""

    def __init__(self, block_size):
        self.block_size = block_size

    def pad(value):
        raise NotImplementedError('Subclasses must implement this!')

    def unpad(value):
        raise NotImplementedError('Subclasses must implement this!')


class PKCS5Padding(Padding):
    """Provide PKCS5 padding and unpadding."""

    def pad(self, value):
        if not isinstance(value, six.binary_type):
            value = value.encode()
        padding_length = (self.block_size - len(value) % self.block_size)
        padding_sequence = padding_length * six.b(chr(padding_length))
        value_with_padding = value + padding_sequence

        return value_with_padding

    def unpad(self, value):
        # Perform some input validations.
        # In case of error, we throw a generic InvalidPaddingError()
        if not value or len(value) < self.block_size:
            # PKCS5 padded output will always be at least 1 block size
            raise InvalidPaddingError()
        if len(value) % self.block_size != 0:
            # PKCS5 padded output will be a multiple of the block size
            raise InvalidPaddingError()
        if isinstance(value, six.binary_type):
            padding_length = value[-1]
        if isinstance(value, six.string_types):
            padding_length = ord(value[-1])
        if padding_length == 0 or padding_length > self.block_size:
            raise InvalidPaddingError()

        def convert_byte_or_char_to_number(x):
            return ord(x) if isinstance(x, six.string_types) else x
        if any([padding_length != convert_byte_or_char_to_number(x)
               for x in value[-padding_length:]]):
            raise InvalidPaddingError()

        value_without_padding = value[0:-padding_length]

        return value_without_padding


class OneAndZeroesPadding(Padding):
    """Provide the one and zeroes padding and unpadding.

    This mechanism pads with 0x80 followed by zero bytes.
    For unpadding it strips off all trailing zero bytes and the 0x80 byte.
    """

    BYTE_80 = 0x80
    BYTE_00 = 0x00

    def pad(self, value):
        if not isinstance(value, six.binary_type):
            value = value.encode()
        padding_length = (self.block_size - len(value) % self.block_size)
        one_part_bytes = six.b(chr(self.BYTE_80))
        zeroes_part_bytes = (padding_length - 1) * six.b(chr(self.BYTE_00))
        padding_sequence = one_part_bytes + zeroes_part_bytes
        value_with_padding = value + padding_sequence

        return value_with_padding

    def unpad(self, value):
        value_without_padding = value.rstrip(six.b(chr(self.BYTE_00)))
        value_without_padding = value_without_padding.rstrip(
            six.b(chr(self.BYTE_80)))

        return value_without_padding


class ZeroesPadding(Padding):
    """Provide zeroes padding and unpadding.

    This mechanism pads with 0x00 except the last byte equals
    to the padding length. For unpadding it reads the last byte
    and strips off that many bytes.
    """

    BYTE_00 = 0x00

    def pad(self, value):
        if not isinstance(value, six.binary_type):
            value = value.encode()
        padding_length = (self.block_size - len(value) % self.block_size)
        zeroes_part_bytes = (padding_length - 1) * six.b(chr(self.BYTE_00))
        last_part_bytes = six.b(chr(padding_length))
        padding_sequence = zeroes_part_bytes + last_part_bytes
        value_with_padding = value + padding_sequence

        return value_with_padding

    def unpad(self, value):
        if isinstance(value, six.binary_type):
            padding_length = value[-1]
        if isinstance(value, six.string_types):
            padding_length = ord(value[-1])
        value_without_padding = value[0:-padding_length]

        return value_without_padding


class NaivePadding(Padding):
    """Naive padding and unpadding using '*'.

    The class is provided only for backwards compatibility.
    """

    CHARACTER = six.b('*')

    def pad(self, value):
        num_of_bytes = (self.block_size - len(value) % self.block_size)
        value_with_padding = value + num_of_bytes * self.CHARACTER

        return value_with_padding

    def unpad(self, value):
        value_without_padding = value.rstrip(self.CHARACTER)

        return value_without_padding


PADDING_MECHANISM = {
    'pkcs5': PKCS5Padding,
    'oneandzeroes': OneAndZeroesPadding,
    'zeroes': ZeroesPadding,
    'naive': NaivePadding
}
