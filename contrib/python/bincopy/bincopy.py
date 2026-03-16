"""Mangling of various file formats that conveys binary information
(Motorola S-Record, Intel HEX, TI-TXT and binary files).

"""

import re
import copy
import binascii
import string
import sys
import argparse
from collections import namedtuple
from io import StringIO
from io import BytesIO

from humanfriendly import format_size
from argparse_addons import Integer
from elftools.elf.elffile import ELFFile
from elftools.elf.constants import SH_FLAGS


__author__ = 'Erik Moqvist'
__version__ = '20.1.1'


DEFAULT_WORD_SIZE_BITS = 8

# Intel hex types.
IHEX_DATA = 0
IHEX_END_OF_FILE = 1
IHEX_EXTENDED_SEGMENT_ADDRESS = 2
IHEX_START_SEGMENT_ADDRESS = 3
IHEX_EXTENDED_LINEAR_ADDRESS = 4
IHEX_START_LINEAR_ADDRESS = 5

# TI-TXT defines
TI_TXT_BYTES_PER_LINE = 16


class Error(Exception):
    """Bincopy base exception.

    """

    pass


class UnsupportedFileFormatError(Error):

    def __str__(self):
        return 'Unsupported file format.'


class AddDataError(Error):
    pass


def crc_srec(hexstr):
    """Calculate the CRC for given Motorola S-Record hexstring.

    """

    crc = sum(binascii.unhexlify(hexstr))
    crc &= 0xff
    crc ^= 0xff

    return crc


def crc_ihex(hexstr):
    """Calculate the CRC for given Intel HEX hexstring.

    """

    crc = sum(binascii.unhexlify(hexstr))
    crc &= 0xff
    crc = ((~crc + 1) & 0xff)

    return crc


def pack_srec(type_, address, size, data):
    """Create a Motorola S-Record record of given data.

    """

    if type_ in '0159':
        line = f'{size + 2 + 1:02X}{address:04X}'
    elif type_ in '268':
        line = f'{size + 3 + 1:02X}{address:06X}'
    elif type_ in '37':
        line = f'{size + 4 + 1:02X}{address:08X}'
    else:
        raise Error(f"expected record type 0..3 or 5..9, but got '{type_}'")

    if data:
        line += binascii.hexlify(data).decode('ascii').upper()

    return f'S{type_}{line}{crc_srec(line):02X}'


def unpack_srec(record):
    """Unpack given Motorola S-Record record into variables.

    """

    # Minimum STSSCC, where T is type, SS is size and CC is crc.
    if len(record) < 6:
        raise Error(f"record '{record}' too short")

    if record[0] != 'S':
        raise Error(f"record '{record}' not starting with an 'S'")

    value = bytearray.fromhex(record[2:])
    size = value[0]

    if size != len(value) - 1:
        raise Error(f"record '{record}' has wrong size")

    type_ = record[1]

    if type_ in '0159':
        width = 2
    elif type_ in '268':
        width = 3
    elif type_ in '37':
        width = 4
    else:
        raise Error(f"expected record type 0..3 or 5..9, but got '{type_}'")

    data_offset = (1 + width)
    address = int.from_bytes(value[1:data_offset], byteorder='big')
    data = value[data_offset:-1]
    actual_crc = value[-1]
    expected_crc = crc_srec(record[2:-2])

    if actual_crc != expected_crc:
        raise Error(
            f"expected crc '{expected_crc:02X}' in record {record}, but got "
            f"'{actual_crc:02X}'")

    return (type_, address, len(data), data)


def pack_ihex(type_, address, size, data):
    """Create a Intel HEX record of given data.

    """

    line = f'{size:02X}{address:04X}{type_:02X}'

    if data:
        line += binascii.hexlify(data).decode('ascii').upper()

    return f':{line}{crc_ihex(line):02X}'


def unpack_ihex(record):
    """Unpack given Intel HEX record into variables.

    """

    # Minimum :SSAAAATTCC, where SS is size, AAAA is address, TT is
    # type and CC is crc.
    if len(record) < 11:
        raise Error(f"record '{record}' too short")

    if record[0] != ':':
        raise Error(f"record '{record}' not starting with a ':'")

    value = bytearray.fromhex(record[1:])
    size = value[0]

    if size != len(value) - 5:
        raise Error(f"record '{record}' has wrong size")

    address = int.from_bytes(value[1:3], byteorder='big')
    type_ = value[3]
    data = value[4:-1]
    actual_crc = value[-1]
    expected_crc = crc_ihex(record[1:-2])

    if actual_crc != expected_crc:
        raise Error(
            f"expected crc '{expected_crc:02X}' in record {record}, but got "
            f"'{actual_crc:02X}'")

    return (type_, address, size, data)


def pretty_srec(record):
    """Make given Motorola S-Record pretty by adding colors to it.

    """

    type_ = record[1:2]

    if type_ == '0':
        width = 4
        type_color = '\033[0;92m'
        type_text = ' (header)'
    elif type_ in '1':
        width = 4
        type_color = '\033[0;32m'
        type_text = ' (data)'
    elif type_ in '2':
        width = 6
        type_color = '\033[0;32m'
        type_text = ' (data)'
    elif type_ in '3':
        width = 8
        type_color = '\033[0;32m'
        type_text = ' (data)'
    elif type_ in '5':
        width = 4
        type_color = '\033[0;93m'
        type_text = ' (count)'
    elif type_ in '6':
        width = 6
        type_color = '\033[0;93m'
        type_text = ' (count)'
    elif type_ in '7':
        width = 8
        type_color = '\033[0;96m'
        type_text = ' (start address)'
    elif type_ in '8':
        width = 6
        type_color = '\033[0;96m'
        type_text = ' (start address)'
    elif type_ in '9':
        width = 4
        type_color = '\033[0;96m'
        type_text = ' (start address)'
    else:
        raise Error(f"expected record type 0..3 or 5..9, but got '{type_}'")

    return (type_color + record[:2]
            + '\033[0;95m' + record[2:4]
            + '\033[0;33m' + record[4:4 + width]
            + '\033[0m' + record[4 + width:-2]
            + '\033[0;36m' + record[-2:]
            + '\033[0m' + type_text)


def pretty_ihex(record):
    """Make given Intel HEX record pretty by adding colors to it.

    """

    type_ = int(record[7:9], 16)

    if type_ == IHEX_DATA:
        type_color = '\033[0;32m'
        type_text = ' (data)'
    elif type_ == IHEX_END_OF_FILE:
        type_color = '\033[0;96m'
        type_text = ' (end of file)'
    elif type_ == IHEX_EXTENDED_SEGMENT_ADDRESS:
        type_color = '\033[0;34m'
        type_text = ' (extended segment address)'
    elif type_ == IHEX_EXTENDED_LINEAR_ADDRESS:
        type_color = '\033[0;96m'
        type_text = ' (extended linear address)'
    elif type_ == IHEX_START_SEGMENT_ADDRESS:
        type_color = '\033[0;92m'
        type_text = ' (start segment address)'
    elif type_ == IHEX_START_LINEAR_ADDRESS:
        type_color = '\033[0;92m'
        type_text = ' (start linear address)'
    else:
        raise Error(f"expected type 1..5 in record {record}, but got {type_}")

    return ('\033[0;31m' + record[:1]
            + '\033[0;95m' + record[1:3]
            + '\033[0;33m' + record[3:7]
            + type_color + record[7:9]
            + '\033[0m' + record[9:-2]
            + '\033[0;36m' + record[-2:]
            + '\033[0m' + type_text)


def pretty_ti_txt(line):
    """Make given TI TXT line pretty by adding colors to it.

    """

    if line.startswith('@'):
        line = '\033[0;33m' + line + '\033[0m (segment address)'
    elif line == 'q':
        line = '\033[0;35m' + line + '\033[0m (end of file)'
    else:
        line += ' (data)'

    return line


def comment_remover(text):
    def replacer(match):
        s = match.group(0)

        if s.startswith('/'):
            return " " # note: a space and not an empty string
        else:
            return s

    pattern = re.compile(
        r'//.*?$|/\*.*?\*/|\'(?:\\.|[^\\\'])*\'|"(?:\\.|[^\\"])*"',
        re.DOTALL | re.MULTILINE)

    return re.sub(pattern, replacer, text)


def is_srec(records):
    try:
        unpack_srec(records.partition('\n')[0].rstrip())
    except Error:
        return False
    else:
        return True


def is_ihex(records):
    try:
        unpack_ihex(records.partition('\n')[0].rstrip())
    except Error:
        return False
    else:
        return True


def is_ti_txt(data):
    try:
        BinFile().add_ti_txt(data)
    except Exception:
        return False
    else:
        return True


def is_verilog_vmem(data):
    try:
        BinFile().add_verilog_vmem(data)
    except Exception:
        return False
    else:
        return True


class Segment:
    """A segment is a chunk data with given minimum and maximum address.

    """

    def __init__(self, minimum_address, maximum_address, data, word_size_bytes):
        self.minimum_address = minimum_address
        self.maximum_address = maximum_address
        self.data = bytearray(data)
        self.word_size_bytes = word_size_bytes

    @property
    def address(self):
        return self.minimum_address // self.word_size_bytes

    def chunks(self, size=32, alignment=1, padding=b''):
        """Yield data chunks of `size` words, aligned as given by `alignment`.

        Each chunk is itself a Segment.

        `size` and `alignment` are in words. `size` must be a multiple of
        `alignment`. If set, `padding` must be a word value.

        If `padding` is set, the first and final chunks are padded so that:
            1. The first chunk is aligned even if the segment itself is not.
            2. The final chunk's size is a multiple of `alignment`.

        """

        if (size % alignment) != 0:
            raise Error(f'size {size} is not a multiple of alignment {alignment}')

        if padding and len(padding) != self.word_size_bytes:
            raise Error(f'padding must be a word value (size {self.word_size_bytes}),'
                        f' got {padding}')

        size *= self.word_size_bytes
        alignment *= self.word_size_bytes
        address = self.minimum_address
        data = self.data

        # Apply padding to first and final chunk, if padding is non-empty.
        align_offset = address % alignment
        address -= align_offset * bool(padding)
        data = align_offset // self.word_size_bytes * padding + data
        data += (alignment - len(data)) % alignment // self.word_size_bytes * padding

        # First chunk may be non-aligned and shorter than `size` if padding is empty.
        chunk_offset = (address % alignment)

        if chunk_offset != 0:
            first_chunk_size = (alignment - chunk_offset)
            yield Segment(address,
                          address + size,
                          data[:first_chunk_size],
                          self.word_size_bytes)
            address += first_chunk_size
            data = data[first_chunk_size:]
        else:
            first_chunk_size = 0

        for offset in range(0, len(data), size):
            yield Segment(address + offset,
                          address + offset + size,
                          data[offset:offset + size],
                          self.word_size_bytes)

    def add_data(self, minimum_address, maximum_address, data, overwrite):
        """Add given data to this segment. The added data must be adjacent to
        the current segment data, otherwise an exception is thrown.

        """

        if minimum_address == self.maximum_address:
            self.maximum_address = maximum_address
            self.data += data
        elif maximum_address == self.minimum_address:
            self.minimum_address = minimum_address
            self.data = data + self.data
        elif (overwrite
              and minimum_address < self.maximum_address
              and maximum_address > self.minimum_address):
            self_data_offset = minimum_address - self.minimum_address

            # Prepend data.
            if self_data_offset < 0:
                self_data_offset *= -1
                self.data = data[:self_data_offset] + self.data
                del data[:self_data_offset]
                self.minimum_address = minimum_address

            # Overwrite overlapping part.
            self_data_left = len(self.data) - self_data_offset

            if len(data) <= self_data_left:
                self.data[self_data_offset:self_data_offset + len(data)] = data
                data = bytearray()
            else:
                self.data[self_data_offset:] = data[:self_data_left]
                data = data[self_data_left:]

            # Append data.
            if len(data) > 0:
                self.data += data
                self.maximum_address = maximum_address
        else:
            raise AddDataError(
                'data added to a segment must be adjacent to or overlapping '
                'with the original segment data')

    def remove_data(self, minimum_address, maximum_address):
        """Remove given data range from this segment. Returns the second
        segment if the removed data splits this segment in two.

        """

        if ((minimum_address >= self.maximum_address)
            or (maximum_address <= self.minimum_address)):
            return

        if minimum_address < self.minimum_address:
            minimum_address = self.minimum_address

        if maximum_address > self.maximum_address:
            maximum_address = self.maximum_address

        remove_size = maximum_address - minimum_address
        part1_size = minimum_address - self.minimum_address
        part1_data = self.data[0:part1_size]
        part2_data = self.data[part1_size + remove_size:]

        if len(part1_data) and len(part2_data):
            # Update this segment and return the second segment.
            self.maximum_address = self.minimum_address + part1_size
            self.data = part1_data

            return Segment(maximum_address,
                           maximum_address + len(part2_data),
                           part2_data,
                           self.word_size_bytes)
        else:
            # Update this segment.
            if len(part1_data) > 0:
                self.maximum_address = minimum_address
                self.data = part1_data
            elif len(part2_data) > 0:
                self.minimum_address = maximum_address
                self.data = part2_data
            else:
                self.maximum_address = self.minimum_address
                self.data = bytearray()

    def __eq__(self, other):
        if isinstance(other, tuple):
            return self.address, self.data == other
        elif isinstance(other, Segment):
            return ((self.minimum_address == other.minimum_address)
                    and (self.maximum_address == other.maximum_address)
                    and (self.data == other.data)
                    and (self.word_size_bytes == other.word_size_bytes))
        else:
            return False

    def __iter__(self):
        # Allows unpacking as ``address, data = segment``.
        yield self.address
        yield self.data

    def __repr__(self):
        return f'Segment(address={self.address}, data={self.data})'

    def __len__(self):
        return len(self.data) // self.word_size_bytes


_Segment = Segment


class Segments:
    """A list of segments.

    """

    def __init__(self, word_size_bytes):
        self.word_size_bytes = word_size_bytes
        self._current_segment = None
        self._current_segment_index = None
        self._list = []

    def __str__(self):
        return '\n'.join([str(s) for s in self._list])

    def __iter__(self):
        """Iterate over all segments.

        """

        for segment in self._list:
            yield segment

    def __getitem__(self, index):
        try:
            return self._list[index]
        except IndexError:
            raise Error('segment does not exist')

    @property
    def minimum_address(self):
        """The minimum address of the data, or ``None`` if no data is
        available.

        """

        if not self._list:
            return None

        return self._list[0].minimum_address

    @property
    def maximum_address(self):
        """The maximum address of the data, or ``None`` if no data is
        available.

        """

        if not self._list:
            return None

        return self._list[-1].maximum_address

    def add(self, segment, overwrite=False):
        """Add segments by ascending address.

        """

        if self._list:
            if segment.minimum_address == self._current_segment.maximum_address:
                # Fast insertion for adjacent segments.
                self._current_segment.add_data(segment.minimum_address,
                                               segment.maximum_address,
                                               segment.data,
                                               overwrite)
            else:
                # Linear insert.
                for i, s in enumerate(self._list):
                    if segment.minimum_address <= s.maximum_address:
                        break

                if segment.minimum_address > s.maximum_address:
                    # Non-overlapping, non-adjacent after.
                    self._list.append(segment)
                elif segment.maximum_address < s.minimum_address:
                    # Non-overlapping, non-adjacent before.
                    self._list.insert(i, segment)
                else:
                    # Adjacent or overlapping.
                    s.add_data(segment.minimum_address,
                               segment.maximum_address,
                               segment.data,
                               overwrite)
                    segment = s

                self._current_segment = segment
                self._current_segment_index = i

            # Remove overwritten and merge adjacent segments.
            while self._current_segment is not self._list[-1]:
                s = self._list[self._current_segment_index + 1]

                if self._current_segment.maximum_address >= s.maximum_address:
                    # The whole segment is overwritten.
                    del self._list[self._current_segment_index + 1]
                elif self._current_segment.maximum_address >= s.minimum_address:
                    # Adjacent or beginning of the segment overwritten.
                    self._current_segment.add_data(
                        self._current_segment.maximum_address,
                        s.maximum_address,
                        s.data[self._current_segment.maximum_address - s.minimum_address:],
                        overwrite=False)
                    del self._list[self._current_segment_index+1]
                    break
                else:
                    # Segments are not overlapping, nor adjacent.
                    break
        else:
            self._list.append(segment)
            self._current_segment = segment
            self._current_segment_index = 0

    def remove(self, minimum_address, maximum_address):
        new_list = []

        for segment in self._list:
            split = segment.remove_data(minimum_address, maximum_address)

            if segment.minimum_address < segment.maximum_address:
                new_list.append(segment)

            if split:
                new_list.append(split)

        self._list = new_list

    def chunks(self, size=32, alignment=1, padding=b''):
        """Iterate over all segments and yield chunks of the data.
        
        The chunks are `size` words long, aligned as given by `alignment`.

        Each chunk is itself a Segment.

        `size` and `alignment` are in words. `size` must be a multiple of
        `alignment`. If set, `padding` must be a word value.

        If `padding` is set, the first and final chunks of each segment are
        padded so that:
            1. The first chunk is aligned even if the segment itself is not.
            2. The final chunk's size is a multiple of `alignment`.

        """

        if (size % alignment) != 0:
            raise Error(f'size {size} is not a multiple of alignment {alignment}')

        if padding and len(padding) != self.word_size_bytes:
            raise Error(f'padding must be a word value (size {self.word_size_bytes}),'
                        f' got {padding}')

        previous = Segment(-1, -1, b'', 1)

        for segment in self:
            for chunk in segment.chunks(size, alignment, padding):
                # When chunks are padded to alignment, the final chunk of the previous
                # segment and the first chunk of the current segment may overlap by
                # one alignment block. To avoid overwriting data from the lower
                # segment, the chunks must be merged.
                if chunk.address < previous.address + len(previous):
                    low = previous.data[-alignment * self.word_size_bytes:]
                    high = chunk.data[:alignment * self.word_size_bytes]
                    merged = int.to_bytes(int.from_bytes(low, 'big') ^
                                          int.from_bytes(high, 'big') ^
                                          int.from_bytes(alignment * padding, 'big'),
                                          alignment * self.word_size_bytes, 'big')
                    chunk.data = merged + chunk.data[alignment * self.word_size_bytes:]

                yield chunk

            previous = chunk

    def __len__(self):
        """Get the number of segments.

        """

        return len(self._list)


_Segments = Segments
    

class BinFile:
    """A binary file.

    `filenames` may be a single file or a list of files. Each file is
    opened and its data added, given that the format is Motorola
    S-Records, Intel HEX or TI-TXT.

    Set `overwrite` to ``True`` to allow already added data to be
    overwritten.

    `word_size_bits` is the number of bits per word.

    `header_encoding` is the encoding used to encode and decode the
    file header (if any). Give as ``None`` to disable encoding,
    leaving the header as an untouched bytes object.

    """

    def __init__(self,
                 filenames=None,
                 overwrite=False,
                 word_size_bits=DEFAULT_WORD_SIZE_BITS,
                 header_encoding='utf-8'):
        if (word_size_bits % 8) != 0:
            raise Error(
                f'word size must be a multiple of 8 bits, but got {word_size_bits} '
                f'bits')

        self.word_size_bits = word_size_bits
        self.word_size_bytes = (word_size_bits // 8)
        self._header_encoding = header_encoding
        self._header = None
        self._execution_start_address = None
        self._segments = Segments(self.word_size_bytes)

        if filenames is not None:
            if isinstance(filenames, str):
                filenames = [filenames]

            for filename in filenames:
                self.add_file(filename, overwrite=overwrite)

    def __setitem__(self, key, data):
        """Write data to given absolute address or address range.

        """

        if isinstance(key, slice):
            if key.start is None:
                address = self.minimum_address
            else:
                address = key.start
        else:
            address = key
            data = hex((0x80 << (8 * self.word_size_bytes)) | data)
            data = binascii.unhexlify(data[4:])

        self.add_binary(data, address, overwrite=True)

    def __getitem__(self, key):
        """Read data from given absolute address or address range.

        """

        if isinstance(key, slice):
            if key.start is None:
                minimum_address = self.minimum_address
            else:
                minimum_address = key.start

            if key.stop is None:
                maximum_address = self.maximum_address
            else:
                maximum_address = key.stop

            return self.as_binary(minimum_address, maximum_address)
        else:
            if key < self.minimum_address or key >= self.maximum_address:
                raise IndexError(f'binary file index {key} out of range')

            return int(binascii.hexlify(self.as_binary(key, key + 1)), 16)


    def __len__(self):
        """Number of words in the file.

        """

        length = sum([len(segment.data) for segment in self.segments])
        length //= self.word_size_bytes

        return length

    def __iadd__(self, other):
        self.add_srec(other.as_srec())

        return self

    def __str__(self):
        return str(self._segments)

    @property
    def execution_start_address(self):
        """The execution start address, or ``None`` if missing.

        """

        return self._execution_start_address

    @execution_start_address.setter
    def execution_start_address(self, address):
        self._execution_start_address = address

    @property
    def minimum_address(self):
        """The minimum address of the data, or ``None`` if the file is empty.

        """

        minimum_address = self._segments.minimum_address

        if minimum_address is not None:
            minimum_address //= self.word_size_bytes

        return minimum_address

    @property
    def maximum_address(self):
        """The maximum address of the data plus one, or ``None`` if the file
        is empty.

        """

        maximum_address = self._segments.maximum_address

        if maximum_address is not None:
            maximum_address //= self.word_size_bytes

        return maximum_address

    @property
    def header(self):
        """The binary file header, or ``None`` if missing. See
        :class:`BinFile's<.BinFile>` `header_encoding` argument for
        encoding options.

        """

        if self._header_encoding is None:
            return self._header
        else:
            return self._header.decode(self._header_encoding)

    @header.setter
    def header(self, header):
        if self._header_encoding is None:
            if not isinstance(header, bytes):
                raise TypeError(f'expected a bytes object, but got {type(header)}')

            self._header = header
        else:
            self._header = header.encode(self._header_encoding)

    @property
    def segments(self):
        """The segments object. Can be used to iterate over all segments in
        the binary.

        Below is an example iterating over all segments, two in this
        case, and printing them.

        >>> for segment in binfile.segments:
        ...     print(segment)
        ...
        Segment(address=0, data=bytearray(b'\\x00\\x01\\x02'))
        Segment(address=10, data=bytearray(b'\\x03\\x04\\x05'))

        All segments can be split into smaller pieces using the
        `chunks(size=32, alignment=1)` method.

        >>> for chunk in binfile.segments.chunks(2):
        ...     print(chunk)
        ...
        Segment(address=0, data=bytearray(b'\\x00\\x01'))
        Segment(address=2, data=bytearray(b'\\x02'))
        Segment(address=10, data=bytearray(b'\\x03\\x04'))
        Segment(address=12, data=bytearray(b'\\x05'))

        Each segment can be split into smaller pieces using the
        `chunks(size=32, alignment=1)` method on a single segment.

        >>> for segment in binfile.segments:
        ...     print(segment)
        ...     for chunk in segment.chunks(2):
        ...         print(chunk)
        ...
        Segment(address=0, data=bytearray(b'\\x00\\x01\\x02'))
        Segment(address=0, data=bytearray(b'\\x00\\x01'))
        Segment(address=2, data=bytearray(b'\\x02'))
        Segment(address=10, data=bytearray(b'\\x03\\x04\\x05'))
        Segment(address=10, data=bytearray(b'\\x03\\x04'))
        Segment(address=12, data=bytearray(b'\\x05'))

        """

        return self._segments

    def add(self, data, overwrite=False):
        """Add given data string by guessing its format. The format must be
        Motorola S-Records, Intel HEX or TI-TXT. Set `overwrite` to
        ``True`` to allow already added data to be overwritten.

        """

        if is_srec(data):
            self.add_srec(data, overwrite)
        elif is_ihex(data):
            self.add_ihex(data, overwrite)
        elif is_ti_txt(data):
            self.add_ti_txt(data, overwrite)
        elif is_verilog_vmem(data):
            self.add_verilog_vmem(data, overwrite)
        else:
            raise UnsupportedFileFormatError()

    def add_srec(self, records, overwrite=False):
        """Add given Motorola S-Records string. Set `overwrite` to ``True`` to
        allow already added data to be overwritten.

        """

        for record in StringIO(records):
            record = record.strip()

            # Ignore blank lines.
            if not record:
                continue

            type_, address, size, data = unpack_srec(record)

            if type_ == '0':
                self._header = data
            elif type_ in '123':
                address *= self.word_size_bytes
                self._segments.add(Segment(address,
                                           address + size,
                                           data,
                                           self.word_size_bytes),
                                   overwrite)
            elif type_ in '789':
                self.execution_start_address = address

    def add_ihex(self, records, overwrite=False):
        """Add given Intel HEX records string. Set `overwrite` to ``True`` to
        allow already added data to be overwritten.

        """

        extended_segment_address = 0
        extended_linear_address = 0

        for record in StringIO(records):
            record = record.strip()

            # Ignore blank lines.
            if not record:
                continue

            type_, address, size, data = unpack_ihex(record)

            if type_ == IHEX_DATA:
                address = (address
                           + extended_segment_address
                           + extended_linear_address)
                address *= self.word_size_bytes
                self._segments.add(Segment(address,
                                           address + size,
                                           data,
                                           self.word_size_bytes),
                                   overwrite)
            elif type_ == IHEX_END_OF_FILE:
                pass
            elif type_ == IHEX_EXTENDED_SEGMENT_ADDRESS:
                extended_segment_address = int(binascii.hexlify(data), 16)
                extended_segment_address *= 16
            elif type_ == IHEX_EXTENDED_LINEAR_ADDRESS:
                extended_linear_address = int(binascii.hexlify(data), 16)
                extended_linear_address <<= 16
            elif type_ in [IHEX_START_SEGMENT_ADDRESS, IHEX_START_LINEAR_ADDRESS]:
                self.execution_start_address = int(binascii.hexlify(data), 16)
            else:
                raise Error(f"expected type 1..5 in record {record}, but got {type_}")

    def add_ti_txt(self, lines, overwrite=False):
        """Add given TI-TXT string `lines`. Set `overwrite` to ``True`` to
        allow already added data to be overwritten.

        """

        address = None
        eof_found = False

        for line in StringIO(lines):
            # Abort if data is found after end of file.
            if eof_found:
                raise Error("bad file terminator")

            line = line.strip()

            if len(line) < 1:
                raise Error("bad line length")

            if line[0] == 'q':
                eof_found = True
            elif line[0] == '@':
                try:
                    address = int(line[1:], 16)
                except ValueError:
                    raise Error("bad section address")
            else:
                # Try to decode the data.
                try:
                    data = bytearray(binascii.unhexlify(line.replace(' ', '')))
                except (TypeError, binascii.Error):
                    raise Error("bad data")

                size = len(data)

                # Check that there are correct number of bytes per
                # line. There should TI_TXT_BYTES_PER_LINE. Only
                # exception is last line of section which may be
                # shorter.
                if size > TI_TXT_BYTES_PER_LINE:
                    raise Error("bad line length")

                if address is None:
                    raise Error("missing section address")

                self._segments.add(Segment(address,
                                           address + size,
                                           data,
                                           self.word_size_bytes),
                                   overwrite)

                if size == TI_TXT_BYTES_PER_LINE:
                    address += size
                else:
                    address = None

        if not eof_found:
            raise Error("missing file terminator")

    def add_verilog_vmem(self, data, overwrite=False):
        address = None
        chunk = b''
        words = re.split(r'\s+', comment_remover(data).strip())
        word_size_bytes = None

        for word in words:
            if not word.startswith('@'):
                length = len(word)

                if (length % 2) != 0:
                    raise Error('Invalid word length.')

                length //= 2

                if word_size_bytes is None:
                    word_size_bytes = length
                elif length != word_size_bytes:
                    raise Error(
                        f'Mixed word lengths {length} and {word_size_bytes}.')

        for word in words:
            if word.startswith('@'):
                if address is not None:
                    self._segments.add(Segment(address,
                                               address + len(chunk),
                                               chunk,
                                               self.word_size_bytes))

                address = int(word[1:], 16) * word_size_bytes
                chunk = b''
            else:
                chunk += bytes.fromhex(word)

        if address is not None and chunk:
            self._segments.add(Segment(address,
                                       address + len(chunk),
                                       chunk,
                                       self.word_size_bytes))

    def add_binary(self, data, address=0, overwrite=False):
        """Add given data at given address. Set `overwrite` to ``True`` to
        allow already added data to be overwritten.

        """

        address *= self.word_size_bytes
        self._segments.add(Segment(address,
                                   address + len(data),
                                   bytearray(data),
                                   self.word_size_bytes),
                           overwrite)

    def add_elf(self, data, overwrite=True):
        """Add given ELF data.

        """

        elffile = ELFFile(BytesIO(data))

        self.execution_start_address = elffile.header['e_entry']

        for segment in elffile.iter_segments():
            if segment['p_type'] != 'PT_LOAD':
                 continue

            segment_address = segment['p_paddr']
            segment_offset = segment['p_offset']
            segment_size = segment['p_filesz']

            for section in elffile.iter_sections():
                offset = section['sh_offset']
                size = section['sh_size']
                address = segment_address + offset - segment_offset

                if size == 0:
                    continue

                if segment_offset <= offset < segment_offset + segment_size:
                    if section['sh_type'] == 'SHT_NOBITS':
                        continue

                    if (section['sh_flags'] & SH_FLAGS.SHF_ALLOC) == 0:
                        continue

                    self._segments.add(Segment(address,
                                               address + size,
                                               data[offset:offset + size],
                                               self.word_size_bytes),
                                       overwrite)

    def add_microchip_hex(self, records, overwrite=False):
        """Add given Microchip HEX data.

        Microchip's HEX format is identical to Intel's except an address in
        the HEX file is twice the actual machine address. For example:

        :02000E00E4C943

            :       Start code
            02      Record contains two data bytes
            000E    Address 0x000E; Machine address is 0x000E // 2 == 0x0007
            00      Record type is data
            E4      Low byte at address 0x0007 is 0xE4
            C9      High byte at address 0x0007 is 0xC9

        Microchip HEX records therefore need to be parsed as if the word size
        is one byte, but the parsed data must be handled as if the word size
        is two bytes. This is true for both 8-bit PICs such as PIC18 and
        16-bit PICs such as PIC24.

        """

        self.word_size_bytes = 1
        self.add_ihex(records, overwrite)
        self.word_size_bytes = 2
        self.segments.word_size_bytes = 2

        for segment in self.segments:
            segment.word_size_bytes = 2

    def add_file(self, filename, overwrite=False):
        """Open given file and add its data by guessing its format. The format
        must be Motorola S-Records, Intel HEX, TI-TXT. Set `overwrite`
        to ``True`` to allow already added data to be overwritten.

        """

        with open(filename, 'r') as fin:
            try:
                data = fin.read()
            except UnicodeDecodeError:
                raise UnsupportedFileFormatError()

        self.add(data, overwrite)

    def add_srec_file(self, filename, overwrite=False):
        """Open given Motorola S-Records file and add its records. Set
        `overwrite` to ``True`` to allow already added data to be
        overwritten.

        """

        with open(filename, 'r') as fin:
            self.add_srec(fin.read(), overwrite)

    def add_ihex_file(self, filename, overwrite=False):
        """Open given Intel HEX file and add its records. Set `overwrite` to
        ``True`` to allow already added data to be overwritten.

        """

        with open(filename, 'r') as fin:
            self.add_ihex(fin.read(), overwrite)

    def add_ti_txt_file(self, filename, overwrite=False):
        """Open given TI-TXT file and add its contents. Set `overwrite` to
        ``True`` to allow already added data to be overwritten.

        """

        with open(filename, 'r') as fin:
            self.add_ti_txt(fin.read(), overwrite)

    def add_verilog_vmem_file(self, filename, overwrite=False):
        """Open given Verilog VMEM file and add its contents. Set `overwrite` to
        ``True`` to allow already added data to be overwritten.

        """

        with open(filename, 'r') as fin:
            self.add_verilog_vmem(fin.read(), overwrite)

    def add_binary_file(self, filename, address=0, overwrite=False):
        """Open given binary file and add its contents. Set `overwrite` to
        ``True`` to allow already added data to be overwritten.

        """

        with open(filename, 'rb') as fin:
            self.add_binary(fin.read(), address, overwrite)

    def add_elf_file(self, filename, overwrite=False):
        """Open given ELF file and add its contents. Set `overwrite` to
        ``True`` to allow already added data to be overwritten.

        """

        with open(filename, 'rb') as fin:
            self.add_elf(fin.read(), overwrite)

    def add_microchip_hex_file(self, filename, overwrite=False):
        """Open given Microchip HEX file and add its contents. Set `overwrite`
        to ``True`` to allow already added data to be overwritten.

        """

        with open(filename, 'r') as fin:
            self.add_microchip_hex(fin.read(), overwrite)

    def as_srec(self, number_of_data_bytes=32, address_length_bits=32):
        """Format the binary file as Motorola S-Records records and return
        them as a string.

        `number_of_data_bytes` is the number of data bytes in each
        record.

        `address_length_bits` is the number of address bits in each
        record.

        >>> print(binfile.as_srec())
        S32500000100214601360121470136007EFE09D219012146017E17C20001FF5F16002148011973
        S32500000120194E79234623965778239EDA3F01B2CA3F0156702B5E712B722B73214601342199
        S5030002FA

        """

        header = []

        if self._header is not None:
            record = pack_srec('0', 0, len(self._header), self._header)
            header.append(record)

        type_ = str((address_length_bits // 8) - 1)

        if type_ not in '123':
            raise Error(f"expected data record type 1..3, but got {type_}")

        data = [pack_srec(type_, address, len(data), data)
                for address, data in self._segments.chunks(
                        number_of_data_bytes // self.word_size_bytes)]
        number_of_records = len(data)

        if number_of_records <= 0xffff:
            footer = [pack_srec('5', number_of_records, 0, None)]
        elif number_of_records <= 0xffffff:
            footer = [pack_srec('6', number_of_records, 0, None)]
        else:
            raise Error(f'too many records {number_of_records}')

        # Add the execution start address.
        if self.execution_start_address is not None:
            if type_ == '1':
                record = pack_srec('9', self.execution_start_address, 0, None)
            elif type_ == '2':
                record = pack_srec('8', self.execution_start_address, 0, None)
            else:
                record = pack_srec('7', self.execution_start_address, 0, None)

            footer.append(record)

        return '\n'.join(header + data + footer) + '\n'

    def as_ihex(self, number_of_data_bytes=32, address_length_bits=32):
        """Format the binary file as Intel HEX records and return them as a
        string.

        `number_of_data_bytes` is the number of data bytes in each
        record.

        `address_length_bits` is the number of address bits in each
        record.

        >>> print(binfile.as_ihex())
        :20010000214601360121470136007EFE09D219012146017E17C20001FF5F16002148011979
        :20012000194E79234623965778239EDA3F01B2CA3F0156702B5E712B722B7321460134219F
        :00000001FF

        """

        def i32hex(address, extended_linear_address, data_address):
            if address > 0xffffffff:
                raise Error(
                    'cannot address more than 4 GB in I32HEX files (32 '
                    'bits addresses)')

            address_upper_16_bits = (address >> 16)
            address &= 0xffff

            # All segments are sorted by address. Update the
            # extended linear address when required.
            if address_upper_16_bits > extended_linear_address:
                extended_linear_address = address_upper_16_bits
                packed = pack_ihex(
                    IHEX_EXTENDED_LINEAR_ADDRESS,
                    0,
                    2,
                    binascii.unhexlify(f'{extended_linear_address:04X}'))
                data_address.append(packed)

            return address, extended_linear_address

        def i16hex(address, extended_segment_address, data_address):
            if address > 16 * 0xffff + 0xffff:
                raise Error(
                    'cannot address more than 1 MB in I16HEX files (20 '
                    'bits addresses)')

            address_lower = (address - 16 * extended_segment_address)

            # All segments are sorted by address. Update the
            # extended segment address when required.
            if address_lower > 0xffff:
                extended_segment_address = (4096 * (address >> 16))

                if extended_segment_address > 0xffff:
                    extended_segment_address = 0xffff

                address_lower = (address - 16 * extended_segment_address)
                packed = pack_ihex(
                    IHEX_EXTENDED_SEGMENT_ADDRESS,
                    0,
                    2,
                    binascii.unhexlify(f'{extended_segment_address:04X}'))
                data_address.append(packed)

            return address_lower, extended_segment_address

        def i8hex(address):
            if address > 0xffff:
                raise Error(
                    'cannot address more than 64 kB in I8HEX files (16 '
                    'bits addresses)')

        data_address = []
        extended_segment_address = 0
        extended_linear_address = 0
        number_of_data_words = number_of_data_bytes // self.word_size_bytes

        for address, data in self._segments.chunks(number_of_data_words):
            if address_length_bits == 32:
                address, extended_linear_address = i32hex(address,
                                                          extended_linear_address,
                                                          data_address)
            elif address_length_bits == 24:
                address, extended_segment_address = i16hex(address,
                                                           extended_segment_address,
                                                           data_address)
            elif address_length_bits == 16:
                i8hex(address)
            else:
                raise Error(f'expected address length 16, 24 or 32, but got '
                            f'{address_length_bits}')

            data_address.append(pack_ihex(IHEX_DATA,
                                          address,
                                          len(data),
                                          data))

        footer = []

        if self.execution_start_address is not None:
            if address_length_bits == 24:
                address = binascii.unhexlify(f'{self.execution_start_address:08X}')
                footer.append(pack_ihex(IHEX_START_SEGMENT_ADDRESS,
                                        0,
                                        4,
                                        address))
            elif address_length_bits == 32:
                address = binascii.unhexlify(f'{self.execution_start_address:08X}')
                footer.append(pack_ihex(IHEX_START_LINEAR_ADDRESS,
                                        0,
                                        4,
                                        address))

        footer.append(pack_ihex(IHEX_END_OF_FILE, 0, 0, None))

        return '\n'.join(data_address + footer) + '\n'

    def as_microchip_hex(self, number_of_data_bytes=32, address_length_bits=32):
        """Format the binary file as Microchip HEX records and return them as a
        string.

        `number_of_data_bytes` is the number of data bytes in each
        record.

        `address_length_bits` is the number of address bits in each
        record.

        >>> print(binfile.as_microchip_hex())
        :20010000214601360121470136007EFE09D219012146017E17C20001FF5F16002148011979
        :20012000194E79234623965778239EDA3F01B2CA3F0156702B5E712B722B7321460134219F
        :00000001FF

        """

        self.word_size_bytes = 1
        self.segments.word_size_bytes = 1

        for segment in self.segments:
            segment.word_size_bytes = 1

        records = self.as_ihex(number_of_data_bytes, address_length_bits)

        self.word_size_bytes = 2
        self.segments.word_size_bytes = 2

        for segment in self.segments:
            segment.word_size_bytes = 2

        return records

    def as_ti_txt(self):
        """Format the binary file as a TI-TXT file and return it as a string.

        >>> print(binfile.as_ti_txt())
        @0100
        21 46 01 36 01 21 47 01 36 00 7E FE 09 D2 19 01
        21 46 01 7E 17 C2 00 01 FF 5F 16 00 21 48 01 19
        19 4E 79 23 46 23 96 57 78 23 9E DA 3F 01 B2 CA
        3F 01 56 70 2B 5E 71 2B 72 2B 73 21 46 01 34 21
        q

        """

        lines = []
        number_of_data_words = TI_TXT_BYTES_PER_LINE // self.word_size_bytes

        for segment in self._segments:
            lines.append(f'@{segment.address:04X}')

            for _, data in segment.chunks(number_of_data_words):
                lines.append(' '.join(f'{byte:02X}' for byte in data))

        lines.append('q')

        return '\n'.join(lines) + '\n'

    def as_verilog_vmem(self):
        """Format the binary file as a Verilog VMEM file and return it as a string.

        >>> print(binfile.as_verilog_vmem())

        """

        lines = []

        if self._header is not None:
            lines.append(f'/* {self.header} */')

        for segment in self._segments:
            for address, data in segment.chunks(32 // self.word_size_bytes):
                words = []

                for i in range(0, len(data), self.word_size_bytes):
                    word = ''

                    for byte in data[i:i + self.word_size_bytes]:
                        word += f'{byte:02X}'

                    words.append(word)

                data_hex = ' '.join(words)
                lines.append(f'@{address:08X} {data_hex}')

        return '\n'.join(lines) + '\n'

    def as_binary(self,
                  minimum_address=None,
                  maximum_address=None,
                  padding=None):
        """Return a byte string of all data within given address range.

        `minimum_address` is the absolute minimum address of the
        resulting binary data (including). By default this is the
        minimum address in the binary.

        `maximum_address` is the absolute maximum address of the
        resulting binary data (excluding). By default this is the
        maximum address in the binary plus one.

        `padding` is the word value of the padding between
        non-adjacent segments. Give as a bytes object of length 1 when
        the word size is 8 bits, length 2 when the word size is 16
        bits, and so on. By default the padding is ``b'\\xff' *
        word_size_bytes``.

        >>> binfile.as_binary()
        bytearray(b'!F\\x016\\x01!G\\x016\\x00~\\xfe\\t\\xd2\\x19\\x01!F\\x01~\\x17\\xc2\\x00\\x01
        \\xff_\\x16\\x00!H\\x01\\x19\\x19Ny#F#\\x96Wx#\\x9e\\xda?\\x01\\xb2\\xca?\\x01Vp+^q+r+s!
        F\\x014!')

        """

        if len(self._segments) == 0:
            return b''

        if minimum_address is None:
            current_maximum_address = self.minimum_address
        else:
            current_maximum_address = minimum_address

        if maximum_address is None:
            maximum_address = self.maximum_address

        if current_maximum_address >= maximum_address:
            return b''

        if padding is None:
            padding = b'\xff' * self.word_size_bytes

        binary = bytearray()

        for address, data in self._segments:
            length = len(data) // self.word_size_bytes

            # Discard data below the minimum address.
            if address < current_maximum_address:
                if address + length <= current_maximum_address:
                    continue

                offset = (current_maximum_address - address) * self.word_size_bytes
                data = data[offset:]
                length = len(data) // self.word_size_bytes
                address = current_maximum_address

            # Discard data above the maximum address.
            if address + length > maximum_address:
                if address < maximum_address:
                    size = (maximum_address - address) * self.word_size_bytes
                    data = data[:size]
                    length = len(data) // self.word_size_bytes
                elif maximum_address >= current_maximum_address:
                    binary += padding * (maximum_address - current_maximum_address)
                    break

            binary += padding * (address - current_maximum_address)
            binary += data
            current_maximum_address = address + length

        return binary

    def as_array(self, minimum_address=None, padding=None, separator=', '):
        """Format the binary file as a string values separated by given
        separator `separator`. This function can be used to generate
        array initialization code for C and other languages.

        `minimum_address` is the absolute minimum address of the
        resulting binary data. By default this is the minimum address
        in the binary.

        `padding` is the word value of the padding between
        non-adjacent segments. Give as a bytes object of length 1 when
        the word size is 8 bits, length 2 when the word size is 16
        bits, and so on. By default the padding is ``b'\\xff' *
        word_size_bytes``.

        >>> binfile.as_array()
        '0x21, 0x46, 0x01, 0x36, 0x01, 0x21, 0x47, 0x01, 0x36, 0x00, 0x7e,
         0xfe, 0x09, 0xd2, 0x19, 0x01, 0x21, 0x46, 0x01, 0x7e, 0x17, 0xc2,
         0x00, 0x01, 0xff, 0x5f, 0x16, 0x00, 0x21, 0x48, 0x01, 0x19, 0x19,
         0x4e, 0x79, 0x23, 0x46, 0x23, 0x96, 0x57, 0x78, 0x23, 0x9e, 0xda,
         0x3f, 0x01, 0xb2, 0xca, 0x3f, 0x01, 0x56, 0x70, 0x2b, 0x5e, 0x71,
         0x2b, 0x72, 0x2b, 0x73, 0x21, 0x46, 0x01, 0x34, 0x21'

        """

        binary_data = self.as_binary(minimum_address,
                                     padding=padding)
        words = []

        for offset in range(0, len(binary_data), self.word_size_bytes):
            word = 0

            for byte in binary_data[offset:offset + self.word_size_bytes]:
                word <<= 8
                word += byte

            words.append(f'0x{word:02x}')

        return separator.join(words)

    def as_hexdump(self):
        """Format the binary file as a hexdump and return it as a string.

        >>> print(binfile.as_hexdump())
        00000100  21 46 01 36 01 21 47 01  36 00 7e fe 09 d2 19 01  |!F.6.!G.6.~.....|
        00000110  21 46 01 7e 17 c2 00 01  ff 5f 16 00 21 48 01 19  |!F.~....._..!H..|
        00000120  19 4e 79 23 46 23 96 57  78 23 9e da 3f 01 b2 ca  |.Ny#F#.Wx#..?...|
        00000130  3f 01 56 70 2b 5e 71 2b  72 2b 73 21 46 01 34 21  |?.Vp+^q+r+s!F.4!|

        """

        # Empty file?
        if len(self) == 0:
            return '\n'

        non_dot_characters = set(string.printable)
        non_dot_characters -= set(string.whitespace)
        non_dot_characters |= set(' ')

        def align_to_line(address):
            return address - (address % (16 // self.word_size_bytes))

        def padding(length):
            return [None] * length

        def format_line(address, data):
            """`data` is a list of integers and None for unused elements.

            """

            data += padding(16 - len(data))
            hexdata = []

            for byte in data:
                if byte is not None:
                    elem = f'{byte:02x}'
                else:
                    elem = '  '

                hexdata.append(elem)

            first_half = ' '.join(hexdata[0:8])
            second_half = ' '.join(hexdata[8:16])
            text = ''

            for byte in data:
                if byte is None:
                    text += ' '
                elif chr(byte) in non_dot_characters:
                    text += chr(byte)
                else:
                    text += '.'

            return (f'{address:08x}  {first_half:23s}  {second_half:23s}  |'
                    f'{text:16s}|')

        # Format one line at a time.
        lines = []
        line_address = align_to_line(self.minimum_address)
        line_data = []

        for chunk in self._segments.chunks(16 // self.word_size_bytes,
                                           16 // self.word_size_bytes):
            aligned_chunk_address = align_to_line(chunk.address)

            if aligned_chunk_address > line_address:
                lines.append(format_line(line_address, line_data))

                if aligned_chunk_address > line_address + 16:
                    lines.append('...')

                line_address = aligned_chunk_address
                line_data = []

            line_data += padding((chunk.address - line_address) * self.word_size_bytes
                                 - len(line_data))
            line_data += [byte for byte in chunk.data]

        lines.append(format_line(line_address, line_data))

        return '\n'.join(lines) + '\n'

    def fill(self, value=None, max_words=None):
        """Fill empty space between segments.

        `value` is the value which is used to fill the empty space. By
        default the value is ``b'\\xff' * word_size_bytes``.

        `max_words` is the maximum number of words to fill between the
        segments. Empty space which larger than this is not
        touched. If ``None``, all empty space is filled.

        """

        if value is None:
            value = b'\xff' * self.word_size_bytes

        previous_segment_maximum_address = None
        fill_segments = []

        for address, data in self._segments:
            address *= self.word_size_bytes
            maximum_address = address + len(data)

            if previous_segment_maximum_address is not None:
                fill_size = address - previous_segment_maximum_address
                fill_size_words = fill_size // self.word_size_bytes

                if max_words is None or fill_size_words <= max_words:
                    fill_segments.append(Segment(
                        previous_segment_maximum_address,
                        previous_segment_maximum_address + fill_size,
                        value * fill_size_words,
                        self.word_size_bytes))

            previous_segment_maximum_address = maximum_address

        for segment in fill_segments:
            self._segments.add(segment)

    def exclude(self, minimum_address, maximum_address):
        """Exclude given range and keep the rest.

        `minimum_address` is the first word address to exclude
        (including).

        `maximum_address` is the last word address to exclude
        (excluding).

        """

        if maximum_address < minimum_address:
            raise Error('bad address range')

        minimum_address *= self.word_size_bytes
        maximum_address *= self.word_size_bytes
        self._segments.remove(minimum_address, maximum_address)

    def crop(self, minimum_address, maximum_address):
        """Keep given range and discard the rest.

        `minimum_address` is the first word address to keep
        (including).

        `maximum_address` is the last word address to keep
        (excluding).

        """

        minimum_address *= self.word_size_bytes
        maximum_address *= self.word_size_bytes
        maximum_address_address = self._segments.maximum_address
        self._segments.remove(0, minimum_address)
        self._segments.remove(maximum_address, maximum_address_address)

    def info(self):
        """Return a string of human readable information about the binary
        file.

        .. code-block:: python

           >>> print(binfile.info())
           Data ranges:

               0x00000100 - 0x00000140 (64 bytes)

        """

        info = ''

        if self._header is not None:
            if self._header_encoding is None:
                header = ''

                for b in self.header:
                    if chr(b) in string.printable:
                        header += chr(b)
                    else:
                        header += f'\\x{b:02x}'
            else:
                header = self.header

            info += f'Header:                  "{header}"\n'

        if self.execution_start_address is not None:
            info += (f'Execution start address: '
                     f'0x{self.execution_start_address:08x}\n')

        info += 'Data ranges:\n\n'

        for address, data in self._segments:
            minimum_address = address
            size = len(data)
            maximum_address = (minimum_address + size // self.word_size_bytes)
            info += 4 * ' '
            info += (f'0x{minimum_address:08x} - 0x{maximum_address:08x} '
                     f'({format_size(size, binary=True)})\n')

        return info

    def layout(self):
        """Return the memory layout as a string.

        .. code-block:: python

           >>> print(binfile.layout())
           0x100                                                      0x140
           ================================================================

        """

        size = self.maximum_address - self.minimum_address
        width = min(80, size)
        chunk_address = self.minimum_address
        chunk_size = size // width
        minimum_address = hex(self.minimum_address)
        maximum_address = hex(self.maximum_address)
        padding = ' ' * max(width - len(minimum_address) - len(maximum_address), 0)
        output = f'{minimum_address}{padding}{maximum_address}\n'

        for i in range(width):
            chunk = copy.deepcopy(self)

            if i < (width - 1):
                maximum_address = chunk_address + chunk_size
            else:
                maximum_address = chunk.maximum_address

            chunk.crop(chunk_address, maximum_address)

            if len(chunk) == 0:
                output += ' '
            elif len(chunk) != (maximum_address - chunk_address):
                output += '-'
            else:
                output += '='

            chunk_address += chunk_size

        return output + '\n'


def _do_info(args):
    for binfile in args.binfile:
        bf = BinFile(header_encoding=args.header_encoding,
                     word_size_bits=args.word_size_bits)
        bf.add_file(binfile)
        print('File:                   ', binfile)
        print(bf.info())
        size = (bf.maximum_address - bf.minimum_address)
        print(f'Data ratio:              {round(100 * len(bf) / size, 2)} %')
        print('Layout:')
        print()
        print('\n'.join(['    ' + line for line in bf.layout().splitlines()]))
        print()


def _convert_input_format_type(value):
    items = value.split(',')
    fmt = items[0]
    args = tuple()

    if fmt == 'binary':
        address = 0

        if len(items) >= 2:
            try:
                address = int(items[1], 0)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"invalid binary address '{items[1]}'")

        args = (address, )
    elif fmt in ['ihex', 'srec', 'auto', 'ti_txt', 'verilog_vmem', 'elf']:
        pass
    else:
        raise argparse.ArgumentTypeError(f"invalid input format '{fmt}'")

    return fmt, args


def _convert_output_format_type(value):
    items = value.split(',')
    fmt = items[0]
    args = tuple()

    if fmt in ['srec', 'ihex', 'ti_txt']:
        number_of_data_bytes = 32
        address_length_bits = 32

        if len(items) >= 2:
            try:
                number_of_data_bytes = int(items[1], 0)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"invalid {fmt} number of data bytes '{items[1]}'")

        if len(items) >= 3:
            try:
                address_length_bits = int(items[2], 0)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"invalid {fmt} address length of '{items[2]}' bits")

        args = (number_of_data_bytes, address_length_bits)
    elif fmt == 'elf':
        raise argparse.ArgumentTypeError(f"invalid output format '{fmt}'")
    elif fmt == 'binary':
        minimum_address = None
        maximum_address = None

        if len(items) >= 2:
            try:
                minimum_address = int(items[1], 0)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"invalid binary minimum address '{items[1]}'")

        if len(items) >= 3:
            try:
                maximum_address = int(items[2], 0)
            except ValueError:
                raise argparse.ArgumentTypeError(
                    f"invalid binary maximum address '{items[2]}'")

        args = (minimum_address, maximum_address)
    elif fmt == 'hexdump':
        pass
    elif fmt == 'verilog_vmem':
        pass
    else:
        raise argparse.ArgumentTypeError(f"invalid output format '{fmt}'")

    return fmt, args


def _do_convert_add_file(bf, input_format, infile, overwrite):
    fmt, args = input_format

    try:
        if fmt == 'auto':
            try:
                bf.add_file(infile, *args, overwrite=overwrite)
            except UnsupportedFileFormatError:
                try:
                    bf.add_elf_file(infile, *args, overwrite=overwrite)
                except:
                    bf.add_binary_file(infile, *args, overwrite=overwrite)
        elif fmt == 'srec':
            bf.add_srec_file(infile, *args, overwrite=overwrite)
        elif fmt == 'ihex':
            bf.add_ihex_file(infile, *args, overwrite=overwrite)
        elif fmt == 'binary':
            bf.add_binary_file(infile, *args, overwrite=overwrite)
        elif fmt == 'ti_txt':
            bf.add_ti_txt_file(infile, *args, overwrite=overwrite)
        elif fmt == 'verilog_vmem':
            bf.add_verilog_vmem_file(infile, *args, overwrite=overwrite)
        elif fmt == 'elf':
            bf.add_elf_file(infile, *args, overwrite=overwrite)
    except AddDataError:
        sys.exit('overlapping segments detected, give --overwrite to overwrite '
                 'overlapping segments')


def _do_convert_as(bf, output_format):
    fmt, args = output_format

    if fmt == 'srec':
        converted = bf.as_srec(*args)
    elif fmt == 'ihex':
        converted = bf.as_ihex(*args)
    elif fmt == 'binary':
        converted = bf.as_binary(*args)
    elif fmt == 'hexdump':
        converted = bf.as_hexdump()
    elif fmt == 'ti_txt':
        converted = bf.as_ti_txt()
    elif fmt == 'verilog_vmem':
        converted = bf.as_verilog_vmem()

    return converted


def _do_convert(args):
    input_formats_missing = len(args.infiles) - len(args.input_format)

    if input_formats_missing < 0:
        sys.exit("found more input formats than input files")

    args.input_format += input_formats_missing * [('auto', tuple())]
    binfile = BinFile(word_size_bits=args.word_size_bits)

    for input_format, infile in zip(args.input_format, args.infiles):
        _do_convert_add_file(binfile, input_format, infile, args.overwrite)

    converted = _do_convert_as(binfile, args.output_format)

    if args.outfile == '-':
        if isinstance(converted, str):
            print(converted, end='')
        else:
            sys.stdout.buffer.write(converted)
    else:
        if isinstance(converted, str):
            with open(args.outfile, 'w') as fout:
                fout.write(converted)
        else:
            with open(args.outfile, 'wb') as fout:
                fout.write(converted)


def _do_pretty(args):
    if args.binfile is None:
        data = sys.stdin.read()
    else:
        with open(args.binfile, 'r') as fin:
            data = fin.read()

    if is_srec(data):
        print('\n'.join([pretty_srec(line) for line in data.splitlines()]))
    elif is_ihex(data):
        print('\n'.join([pretty_ihex(line) for line in data.splitlines()]))
    elif is_ti_txt(data):
        print('\n'.join([pretty_ti_txt(line) for line in data.splitlines()]))
    else:
        raise UnsupportedFileFormatError()


def _do_as_srec(args):
    for binfile in args.binfile:
        bf = BinFile()
        bf.add_file(binfile)
        print(bf.as_srec(), end='')


def _do_as_ihex(args):
    for binfile in args.binfile:
        bf = BinFile()
        bf.add_file(binfile)
        print(bf.as_ihex(), end='')


def _do_as_hexdump(args):
    for binfile in args.binfile:
        bf = BinFile()
        bf.add_file(binfile)
        print(bf.as_hexdump(), end='')


def _do_as_ti_txt(args):
    for binfile in args.binfile:
        bf = BinFile()
        bf.add_file(binfile)
        print(bf.as_ti_txt(), end='')


def _do_as_verilog_vmem(args):
    for binfile in args.binfile:
        bf = BinFile()
        bf.add_file(binfile)
        print(bf.as_verilog_vmem(), end='')


def _do_fill(args):
    with open(args.infile, 'r') as fin:
        data = fin.read()

    bf = BinFile()
    bf.add(data)
    bf.fill(args.value.to_bytes(1, 'big'), args.max_words)

    if is_srec(data):
        data = bf.as_srec()
    elif is_ihex(data):
        data = bf.as_ihex()
    elif is_ti_txt(data):
        data = bf.as_ti_txt()
    else:
        raise UnsupportedFileFormatError()

    if args.outfile == '-':
        sys.stdout.write(data)
    else:
        if args.outfile is None:
            outfile = args.infile
        else:
            outfile = args.outfile

        with open(outfile, 'w') as fout:
            fout.write(data)


def _main():
    parser = argparse.ArgumentParser(
        description='Various binary file format utilities.')

    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('--version',
                        action='version',
                        version=__version__,
                        help='Print version information and exit.')

    # Workaround to make the subparser required in Python 3.
    subparsers = parser.add_subparsers(title='subcommands',
                                       dest='subcommand')
    subparsers.required = True

    # The 'info' subparser.
    subparser = subparsers.add_parser(
        'info',
        description='Print general information about given file(s).')
    subparser.add_argument('-e', '--header-encoding',
                           help=('File header encoding. Common encodings '
                                 'include utf-8 and ascii.'))
    subparser.add_argument(
        '-s', '--word-size-bits',
        default=8,
        type=int,
        help='Word size in number of bits (default: %(default)s).')
    subparser.add_argument('binfile',
                           nargs='+',
                           help='One or more binary format files.')
    subparser.set_defaults(func=_do_info)

    # The 'convert' subparser.
    subparser = subparsers.add_parser(
        'convert',
        description='Convert given file(s) to a single file.')
    subparser.add_argument(
        '-i', '--input-format',
        action='append',
        default=[],
        type=_convert_input_format_type,
        help=('Input format auto, srec, ihex, ti_txt, verilog_vmem, elf, or '
              'binary[,<address>] (default: auto). This argument may be repeated, '
              'selecting the input format for each input file.'))
    subparser.add_argument(
        '-o', '--output-format',
        default='hexdump',
        type=_convert_output_format_type,
        help=('Output format srec, ihex, ti_txt, verilog_vmem, binary or hexdump '
              '(default: %(default)s).'))
    subparser.add_argument(
        '-s', '--word-size-bits',
        default=8,
        type=int,
        help='Word size in number of bits (default: %(default)s).')
    subparser.add_argument('-w', '--overwrite',
                           action='store_true',
                           help='Overwrite overlapping data segments.')
    subparser.add_argument('infiles',
                           nargs='+',
                           help='One or more binary format files.')
    subparser.add_argument('outfile',
                           help='Output file, or - to print to standard output.')
    subparser.set_defaults(func=_do_convert)

    # The 'pretty' subparser.
    subparser = subparsers.add_parser(
        'pretty',
        description='Make given binary format file pretty by colorizing it.')
    subparser.add_argument(
        'binfile',
        nargs='?',
        help='A binary format file, or omitted to read from stdin.')
    subparser.set_defaults(func=_do_pretty)

    # The 'as_srec' subparser.
    subparser = subparsers.add_parser(
        'as_srec',
        description='Print given file(s) as Motorola S-records.')
    subparser.add_argument('binfile',
                           nargs='+',
                           help='One or more binary format files.')
    subparser.set_defaults(func=_do_as_srec)

    # The 'as_ihex' subparser.
    subparser = subparsers.add_parser(
        'as_ihex',
        description='Print given file(s) as Intel HEX.')
    subparser.add_argument('binfile',
                           nargs='+',
                           help='One or more binary format files.')
    subparser.set_defaults(func=_do_as_ihex)

    # The 'as_hexdump' subparser.
    subparser = subparsers.add_parser(
        'as_hexdump',
        description='Print given file(s) as hexdumps.')
    subparser.add_argument('binfile',
                           nargs='+',
                           help='One or more binary format files.')
    subparser.set_defaults(func=_do_as_hexdump)

    # The 'as_ti_txt' subparser.
    subparser = subparsers.add_parser(
        'as_ti_txt',
        description='Print given file(s) as TI-TXT.')
    subparser.add_argument('binfile',
                           nargs='+',
                           help='One or more binary format files.')
    subparser.set_defaults(func=_do_as_ti_txt)

    # The 'as_verilog_vmem' subparser.
    subparser = subparsers.add_parser(
        'as_verilog_vmem',
        description='Print given file(s) as Verilog VMEM.')
    subparser.add_argument('binfile',
                           nargs='+',
                           help='One or more binary format files.')
    subparser.set_defaults(func=_do_as_verilog_vmem)

    # The 'fill' subparser.
    subparser = subparsers.add_parser(
        'fill',
        description='Fill empty space between segments.')
    subparser.add_argument(
        '-v', '--value',
        type=Integer(0, 255),
        default=255,
        help=('The value which is used to fill the empty space. Must be in '
              'the range 0..255 (default: %(default)s).'))
    subparser.add_argument(
        '-m', '--max-words',
        type=Integer(0, None),
        help=('The maximum number of words to fill between the segments. Empty '
              'space which larger than this is not touched.'))
    subparser.add_argument('infile',
                           help='Binary format file to fill.')
    subparser.add_argument(
        'outfile',
        nargs='?',
        help=('Output file, or - to print to standard output. Modifies the '
              'input file if omitted.'))
    subparser.set_defaults(func=_do_fill)

    args = parser.parse_args()

    if args.debug:
        args.func(args)
    else:
        try:
            args.func(args)
        except BaseException as e:
            sys.exit('error: ' + str(e))


if __name__ == '__main__':
    _main()
