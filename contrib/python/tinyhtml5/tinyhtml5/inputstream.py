import codecs
import re
from io import BytesIO, StringIO
from pathlib import Path
from string import ascii_letters, ascii_uppercase

import webencodings

from .constants import EOF, ReparseError, space_characters

# Non-unicode versions of constants for use in the pre-parser.
space_characters_bytes = frozenset(item.encode() for item in space_characters)
ascii_letters_bytes = frozenset(item.encode() for item in ascii_letters)
ascii_uppercase_bytes = frozenset(item.encode() for item in ascii_uppercase)
spaces_angle_brackets = space_characters_bytes | frozenset([b">", b"<"])

invalid_unicode_re = re.compile(
    "[\u0001-\u0008\u000B\u000E-\u001F\u007F-\u009F\uFDD0-\uFDEF\uFFFE\uFFFF"
    "\U0001FFFE\U0001FFFF\U0002FFFE\U0002FFFF\U0003FFFE\U0003FFFF\U0004FFFE"
    "\U0004FFFF\U0005FFFE\U0005FFFF\U0006FFFE\U0006FFFF\U0007FFFE\U0007FFFF"
    "\U0008FFFE\U0008FFFF\U0009FFFE\U0009FFFF\U000AFFFE\U000AFFFF\U000BFFFE"
    "\U000BFFFF\U000CFFFE\U000CFFFF\U000DFFFE\U000DFFFF\U000EFFFE\U000EFFFF"
    "\U000FFFFE\U000FFFFF\U0010FFFE\U0010FFFF\uD800-\uDFFF]")

non_bmp_invalid_codepoints = {
    0x1FFFE, 0x1FFFF, 0x2FFFE, 0x2FFFF, 0x3FFFE, 0x3FFFF, 0x4FFFE, 0x4FFFF,
    0x5FFFE, 0x5FFFF, 0x6FFFE, 0x6FFFF, 0x7FFFE, 0x7FFFF, 0x8FFFE, 0x8FFFF,
    0x9FFFE, 0x9FFFF, 0xAFFFE, 0xAFFFF, 0xBFFFE, 0xBFFFF, 0xCFFFE, 0xCFFFF,
    0xDFFFE, 0xDFFFF, 0xEFFFE, 0xEFFFF, 0xFFFFE, 0xFFFFF, 0x10FFFE, 0x10FFFF}

ascii_punctuation_re = re.compile(
    "[\u0009-\u000D\u0020-\u002F\u003A-\u0040\u005C\u005B-\u0060\u007B-\u007E]")

# Cache for chars_until().
characters_until_regex = {}


def HTMLInputStream(source, **kwargs):  # noqa: N802
    if isinstance(source, str) and len(source) < 200 and Path(source).is_file():
        return HTMLUnicodeInputStream(Path(source).read_text(), **kwargs)
    elif isinstance(source, Path):
        return HTMLUnicodeInputStream(source.read_text(), **kwargs)
    elif isinstance(source.read(0) if hasattr(source, "read") else source, str):
        return HTMLUnicodeInputStream(source, **kwargs)
    else:
        return HTMLBinaryInputStream(source, **kwargs)


class HTMLUnicodeInputStream:
    """Provides a Unicode stream of characters to the HTMLTokenizer.

    This class takes care of character encoding and removing or replacing
    incorrect byte-sequences and also provides column and line tracking.

    """

    def __init__(self, source, **kwargs):
        """Initialise the HTMLInputStream.

        Create a normalized stream from source for use by tinyhtml5.

        source can be either a file-object, local filename or a string.

        """
        # List of where new lines occur.
        self.new_lines = [0]

        self.encoding = (lookup_encoding("utf-8"), "certain")
        self.stream = self.open_stream(source)

        self.reset()

    def reset(self):
        self.chunk = ""
        self.chunk_size = 0
        self.chunk_offset = 0
        self.errors = []

        # Number of (complete) lines in previous chunks.
        self.previous_number_lines = 0
        # Number of columns in the last line of the previous chunk.
        self.previous_number_columns = 0

        # Deal with CR LF and surrogates split over chunk boundaries.
        self._buffered_character = None

    def open_stream(self, source):
        """Produce a file object from source.

        source can be either a file object, local filename or a string.

        """
        return source if hasattr(source, 'read') else StringIO(source)

    def _position(self, offset):
        chunk = self.chunk
        number_lines = chunk.count('\n', 0, offset)
        position_line = self.previous_number_lines + number_lines
        last_line_position = chunk.rfind('\n', 0, offset)
        if last_line_position == -1:
            position_column = self.previous_number_columns + offset
        else:
            position_column = offset - (last_line_position + 1)
        return (position_line, position_column)

    def position(self):
        """Return (line, col) of the current position in the stream."""
        line, column = self._position(self.chunk_offset)
        return (line + 1, column)

    def character(self):
        """Read one character from the stream or queue if available.

        Return EOF when EOF is reached.

        """
        # Read a new chunk from the input stream if necessary.
        if self.chunk_offset >= self.chunk_size:
            if not self.read_chunk():
                return EOF

        chunk_offset = self.chunk_offset
        character = self.chunk[chunk_offset]
        self.chunk_offset = chunk_offset + 1

        return character

    def read_chunk(self):
        self.previous_number_lines, self.previous_number_columns = self._position(
            self.chunk_size)

        self.chunk = ""
        self.chunk_size = 0
        self.chunk_offset = 0

        data = self.stream.read(10240)

        # Deal with CR LF and surrogates broken across chunks.
        if self._buffered_character:
            data = self._buffered_character + data
            self._buffered_character = None
        elif not data:
            # We have no more data, bye-bye stream.
            return False

        if len(data) > 1:
            last = ord(data[-1])
            if last == 0x0D or 0xD800 <= last <= 0xDBFF:
                self._buffered_character = data[-1]
                data = data[:-1]

        # Report character errors.
        for _ in range(len(invalid_unicode_re.findall(data))):
            self.errors.append("invalid-codepoint")

        # Replace invalid characters.
        data = data.replace("\r\n", "\n")
        data = data.replace("\r", "\n")

        self.chunk = data
        self.chunk_size = len(data)

        return True

    def chars_until(self, characters, opposite=False):
        """Return a string of characters from the stream.

        String goes up to but does not include any character in 'characters' or
        EOF. 'characters' must be a container that supports the 'in' method and
        iteration over its characters.

        """

        # Use a cache of regexps to find the required characters.
        try:
            characters = characters_until_regex[(characters, opposite)]
        except KeyError:
            regex = "".join([f"\\x{ord(character):02x}" for character in characters])
            if not opposite:
                regex = f"^{regex}"
            regex = re.compile(f"[{regex}]+")
            characters = characters_until_regex[(characters, opposite)] = regex

        result = []

        while True:
            # Find the longest matching prefix
            match = characters.match(self.chunk, self.chunk_offset)
            if match is None:
                # If nothing matched, and it wasn't because we ran out of
                # chunk, then stop.
                if self.chunk_offset != self.chunk_size:
                    break
            else:
                end = match.end()
                # If not the whole chunk matched, return everything up to the
                # part that didn't match.
                if end != self.chunk_size:
                    result.append(self.chunk[self.chunk_offset:end])
                    self.chunk_offset = end
                    break
            # If the whole remainder of the chunk matched, use it all and read
            # the next chunk.
            result.append(self.chunk[self.chunk_offset:])
            if not self.read_chunk():
                # Reached EOF.
                break

        return "".join(result)

    def unget(self, char):
        # Only one character is allowed to be ungotten at once - it must be
        # consumed again before any further call to unget.
        if char is not EOF:
            if self.chunk_offset == 0:
                # unget is called quite rarely, so it's a good idea to do more
                # work here if it saves a bit of work in the frequently called
                # char and chars_until. So, just prepend the ungotten character
                # onto the current chunk.
                self.chunk = char + self.chunk
                self.chunk_size += 1
            else:
                self.chunk_offset -= 1
                assert self.chunk[self.chunk_offset] == char


class HTMLBinaryInputStream(HTMLUnicodeInputStream):
    """Provide a binary stream of characters to the HTMLTokenizer.

    This class takes care of character encoding and removing or replacing
    incorrect byte-sequences and also provides column and line tracking.

    """

    def __init__(self, source, override_encoding=None, transport_encoding=None,
                 same_origin_parent_encoding=None, likely_encoding=None,
                 default_encoding="windows-1252", **kwargs):
        # Raw Stream - for Unicode objects this will encode to UTF-8 and set
        # self.encoding as appropriate.
        self.raw_stream = self.open_stream(source)

        # Encoding Information.
        # Number of bytes to use when looking for a meta element with
        # encoding information.
        self.number_bytes_meta = 1024
        # Encodings given as arguments.
        self.override_encoding = override_encoding
        self.transport_encoding = transport_encoding
        self.same_origin_parent_encoding = same_origin_parent_encoding
        self.likely_encoding = likely_encoding
        self.default_encoding = default_encoding

        # Determine encoding.
        self.encoding = self.determine_encoding()
        assert self.encoding[0] is not None

        # Reset and set Unicode stream.
        self.reset()

    def reset(self):
        streamreader = self.encoding[0].codec_info.streamreader
        self.stream = streamreader(self.raw_stream, 'replace')
        super().reset()

    def open_stream(self, source):
        if hasattr(source, 'read'):
            if hasattr(source, 'seekable') and source.seekable():
                return source
            source = source.read()
        return BytesIO(source)

    def determine_encoding(self):
        # BOMs take precedence over everything. This will also read past the
        # BOM if present.
        encoding = self.detect_bom(), "certain"
        if encoding[0] is not None:
            return encoding

        # If we've been overridden, we've been overridden.
        encoding = lookup_encoding(self.override_encoding), "certain"
        if encoding[0] is not None:
            return encoding

        # Now check the transport layer.
        encoding = lookup_encoding(self.transport_encoding), "certain"
        if encoding[0] is not None:
            return encoding

        # Look for meta elements with encoding information.
        encoding = self.detect_encoding_meta(), "tentative"
        if encoding[0] is not None:
            return encoding

        # Parent document encoding.
        encoding = lookup_encoding(self.same_origin_parent_encoding), "tentative"
        if encoding[0] is not None and not encoding[0].name.startswith("utf-16"):
            return encoding

        # "likely" encoding.
        encoding = lookup_encoding(self.likely_encoding), "tentative"
        if encoding[0] is not None:
            return encoding

        # Try the default encoding.
        encoding = lookup_encoding(self.default_encoding), "tentative"
        if encoding[0] is not None:
            return encoding

        # Fallback to tinyhtml5's default if even that hasn't worked.
        return lookup_encoding("windows-1252"), "tentative"

    def change_encoding(self, new_encoding):
        assert self.encoding[1] != "certain"
        if (new_encoding := lookup_encoding(new_encoding)) is None:
            return
        if new_encoding.name in ("utf-16be", "utf-16le"):
            new_encoding = lookup_encoding("utf-8")
            assert new_encoding is not None
        elif new_encoding == self.encoding[0]:
            self.encoding = (self.encoding[0], "certain")
        else:
            self.raw_stream.seek(0)
            self.encoding = (new_encoding, "certain")
            self.reset()
            raise ReparseError(
                f"Encoding changed from {self.encoding[0]} to {new_encoding}")

    def detect_bom(self):
        """Attempt to detect at BOM at the start of the stream.

        If an encoding can be determined from the BOM return the name of the
        encoding otherwise return None.

        """
        boms = {
            codecs.BOM_UTF8: 'utf-8',
            codecs.BOM_UTF16_LE: 'utf-16le',
            codecs.BOM_UTF16_BE: 'utf-16be',
            codecs.BOM_UTF32_LE: 'utf-32le',
            codecs.BOM_UTF32_BE: 'utf-32be',
        }

        # Go to beginning of file and read in 4 bytes.
        string = self.raw_stream.read(4)
        assert isinstance(string, bytes)

        # Try detecting the BOM using bytes from the string.
        for seek in (3, 4, 2):  # UTF-8, UTF-32, UTF-16
            if encoding := boms.get(string[:seek]):
                # Set the read position past the BOM if one was found.
                self.raw_stream.seek(seek)
                return lookup_encoding(encoding)

        # Otherwise, set it to the start of the stream.
        self.raw_stream.seek(0)

    def detect_encoding_meta(self):
        """Report the encoding declared by the meta element."""
        buffer = self.raw_stream.read(self.number_bytes_meta)
        assert isinstance(buffer, bytes)
        parser = EncodingParser(buffer)
        self.raw_stream.seek(0)
        encoding = parser.get_encoding()

        if encoding is not None and encoding.name in ("utf-16be", "utf-16le"):
            encoding = lookup_encoding("utf-8")

        return encoding


class EncodingBytes(bytes):
    """Bytes-like object with an associated position and various extra methods.

    If the position is ever greater than the string length then an exception is
    raised.

    """

    def __new__(cls, value):
        assert isinstance(value, bytes)
        return bytes.__new__(cls, value.lower())

    def __init__(self, value):
        self._position = -1

    def __next__(self):
        position = self._position = self._position + 1
        if position >= len(self):
            raise StopIteration
        return self[position:position + 1]

    def previous(self):
        self._position = position = self._position - 1
        return self[position:position + 1]

    def set_position(self, position):
        if self._position >= len(self):
            raise StopIteration
        self._position = max(0, position)

    def get_position(self):
        if self._position >= len(self):
            raise StopIteration
        if self._position >= 0:
            return self._position

    position = property(get_position, set_position)

    @property
    def current_byte(self):
        return self[self.position:self.position + 1]

    def skip(self, characters=space_characters_bytes):
        """Skip past a list of characters."""
        position = self.position  # Use property for the error-checking
        while position < len(self):
            character = self[position:position + 1]
            if character not in characters:
                self._position = position
                return character
            position += 1
        self._position = position
        return None

    def skip_until(self, characters):
        position = self.position
        while position < len(self):
            character = self[position:position + 1]
            if character in characters:
                self._position = position
                return character
            position += 1
        self._position = position
        return None

    def match_bytes(self, bytes):
        """Look for a sequence of bytes at the start of a string.

        If the bytes are found return True and advance the position to the byte
        after the match. Otherwise return False and leave the position alone.

        """
        if result := self.startswith(bytes, self.position):
            self.position += len(bytes)
        return result

    def jump_to(self, bytes):
        """Look for the next sequence of bytes matching a given sequence.

        If a match is found advance the position to the last byte of the match.

        """
        try:
            self._position = self.index(bytes, self.position) + len(bytes) - 1
        except ValueError:
            raise StopIteration
        return True


class EncodingParser:
    """Mini parser for detecting character encoding from meta elements."""

    def __init__(self, data):
        self.data = EncodingBytes(data)
        self.encoding = None

    def get_encoding(self):
        if b"<meta" not in self.data:
            return None

        method_dispatch = {
            b"<!--": self.handle_comment,
            b"<meta": self.handle_meta,
            b"</": self.handle_possible_end_tag,
            b"<!": self.handle_other,
            b"<?": self.handle_other,
            b"<": self.handle_possible_start_tag,
        }
        for _ in self.data:
            keep_parsing = True
            try:
                self.data.jump_to(b"<")
            except StopIteration:
                break
            for key, method in method_dispatch.items():
                if self.data.match_bytes(key):
                    try:
                        keep_parsing = method()
                        break
                    except StopIteration:
                        keep_parsing = False
                        break
            if not keep_parsing:
                break

        return self.encoding

    def handle_comment(self):
        """Skip over comments."""
        return self.data.jump_to(b"-->")

    def handle_meta(self):
        if self.data.current_byte not in space_characters_bytes:
            # If we have <meta not followed by a space so just keep going.
            return True
        # We have a valid meta element we want to search for attributes.
        has_pragma = False
        pending_encoding = None
        while True:
            # Try to find the next attribute after the current position.
            if (attribute := self.get_attribute()) is None:
                return True

            if attribute[0] == b"http-equiv":
                has_pragma = attribute[1] == b"content-type"
                if has_pragma and pending_encoding is not None:
                    self.encoding = pending_encoding
                    return False
            elif attribute[0] == b"charset":
                tentative_encoding = attribute[1]
                codec = lookup_encoding(tentative_encoding)
                if codec is not None:
                    self.encoding = codec
                    return False
            elif attribute[0] == b"content":
                content_parser = ContentAttributeParser(EncodingBytes(attribute[1]))
                if (tentative_encoding := content_parser.parse()) is not None:
                    codec = lookup_encoding(tentative_encoding)
                    if codec is not None:
                        if has_pragma:
                            self.encoding = codec
                            return False
                        pending_encoding = codec

    def handle_possible_start_tag(self):
        return self.handle_possible_tag(end_tag=False)

    def handle_possible_end_tag(self):
        next(self.data)
        return self.handle_possible_tag(end_tag=True)

    def handle_possible_tag(self, end_tag):
        data = self.data
        if data.current_byte not in ascii_letters_bytes:
            # If the next byte is not an ASCII letter either ignore this
            # fragment (possible start tag case) or treat it according to
            # handle_other.
            if end_tag:
                data.previous()
                self.handle_other()
            return True

        character = data.skip_until(spaces_angle_brackets)
        if character == b"<":
            # Return to the first step in the overall "two step" algorithm
            # reprocessing the < byte.
            data.previous()
        else:
            # Read all attributes.
            while True:
                if self.get_attribute() is None:
                    break
        return True

    def handle_other(self):
        return self.data.jump_to(b">")

    def get_attribute(self):
        """Return a (name, value) pair for the next attribute in the stream.

        If no attribute is found, return None.

        """
        data = self.data
        # Step 1 (skip characters).
        character = data.skip(space_characters_bytes | frozenset([b"/"]))
        assert character is None or len(character) == 1
        # Step 2.
        if character in (b">", None):
            return None
        # Step 3.
        attribute_name = []
        attribute_value = []
        # Step 4 attribute name.
        while True:
            if character == b"=" and attribute_name:
                break
            elif character in space_characters_bytes:
                # Step 6!
                character = data.skip()
                break
            elif character in (b"/", b">"):
                return b"".join(attribute_name), b""
            elif character in ascii_uppercase_bytes:
                attribute_name.append(character.lower())
            elif character is None:
                return None
            else:
                attribute_name.append(character)
            # Step 5.
            character = next(data)
        # Step 7.
        if character != b"=":
            data.previous()
            return b"".join(attribute_name), b""
        # Step 8.
        next(data)
        # Step 9
        character = data.skip()
        # Step 10
        if (quote := character) in (b"'", b'"'):
            # 10.1.
            while True:
                # 10.2.
                character = next(data)
                # 10.3.
                if character == quote:
                    next(data)
                    return b"".join(attribute_name), b"".join(attribute_value)
                # 10.4.
                elif character in ascii_uppercase_bytes:
                    attribute_value.append(character.lower())
                # 10.5.
                else:
                    attribute_value.append(character)
        elif character == b">":
            return b"".join(attribute_name), b""
        elif character in ascii_uppercase_bytes:
            attribute_value.append(character.lower())
        elif character is None:
            return None
        else:
            attribute_value.append(character)
        # Step 11.
        while True:
            character = next(data)
            if character in spaces_angle_brackets:
                return b"".join(attribute_name), b"".join(attribute_value)
            elif character in ascii_uppercase_bytes:
                attribute_value.append(character.lower())
            elif character is None:
                return None
            else:
                attribute_value.append(character)


class ContentAttributeParser:
    def __init__(self, data):
        assert isinstance(data, bytes)
        self.data = data

    def parse(self):
        try:
            # Check if the attribute name is charset, otherwise return.
            self.data.jump_to(b"charset")
            self.data.position += 1
            self.data.skip()
            if not self.data.current_byte == b"=":
                # If there is no = sign, keep looking for attributes.
                return None
            self.data.position += 1
            self.data.skip()
            # Look for an encoding between matching quote marks.
            if self.data.current_byte in (b'"', b"'"):
                quote = self.data.current_byte
                self.data.position += 1
                old_position = self.data.position
                if self.data.jump_to(quote):
                    return self.data[old_position:self.data.position]
                else:
                    return None
            else:
                # Unquoted value.
                old_position = self.data.position
                try:
                    self.data.skip_until(space_characters_bytes)
                    return self.data[old_position:self.data.position]
                except StopIteration:
                    # Return the whole remaining value.
                    return self.data[old_position:]
        except StopIteration:
            return None


def lookup_encoding(encoding):
    """Return the Python codec name corresponding to an encoding.

    Return None if the string doesn't correspond to a valid encoding.

    """
    if isinstance(encoding, bytes):
        try:
            encoding = encoding.decode("ascii")
        except UnicodeDecodeError:
            return None

    if encoding is not None:
        try:
            return webencodings.lookup(encoding)
        except AttributeError:
            return None
