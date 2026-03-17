# encoding: utf-8

"""Objects related to system font file lookup."""

import os
import sys

from struct import calcsize, unpack_from

from ..util import lazyproperty


class FontFiles(object):
    """
    A class-based singleton serving as a lazy cache for system font details.
    """

    _font_files = None

    @classmethod
    def find(cls, family_name, is_bold, is_italic):
        """
        Return the absolute path to the installed OpenType font having
        *family_name* and the styles *is_bold* and *is_italic*.
        """
        if cls._font_files is None:
            cls._font_files = cls._installed_fonts()
        return cls._font_files[(family_name, is_bold, is_italic)]

    @classmethod
    def _installed_fonts(cls):
        """
        Return a dict mapping a font descriptor to its font file path,
        containing all the font files resident on the current machine. The
        font descriptor is a (family_name, is_bold, is_italic) 3-tuple.
        """
        fonts = {}
        for d in cls._font_directories():
            for key, path in cls._iter_font_files_in(d):
                fonts[key] = path
        return fonts

    @classmethod
    def _font_directories(cls):
        """
        Return a sequence of directory paths likely to contain fonts on the
        current platform.
        """
        if sys.platform.startswith("darwin"):
            return cls._os_x_font_directories()
        if sys.platform.startswith("win32"):
            return cls._windows_font_directories()
        raise OSError("unsupported operating system")

    @classmethod
    def _iter_font_files_in(cls, directory):
        """
        Generate the OpenType font files found in and under *directory*. Each
        item is a key/value pair. The key is a (family_name, is_bold,
        is_italic) 3-tuple, like ('Arial', True, False), and the value is the
        absolute path to the font file.
        """
        for root, dirs, files in os.walk(directory):
            for filename in files:
                file_ext = os.path.splitext(filename)[1]
                if file_ext.lower() not in (".otf", ".ttf"):
                    continue
                path = os.path.abspath(os.path.join(root, filename))
                with _Font.open(path) as f:
                    yield ((f.family_name, f.is_bold, f.is_italic), path)

    @classmethod
    def _os_x_font_directories(cls):
        """
        Return a sequence of directory paths on a Mac in which fonts are
        likely to be located.
        """
        os_x_font_dirs = [
            "/Library/Fonts",
            "/Network/Library/Fonts",
            "/System/Library/Fonts",
        ]
        home = os.environ.get("HOME")
        if home is not None:
            os_x_font_dirs.extend(
                [os.path.join(home, "Library", "Fonts"), os.path.join(home, ".fonts")]
            )
        return os_x_font_dirs

    @classmethod
    def _windows_font_directories(cls):
        """
        Return a sequence of directory paths on Windows in which fonts are
        likely to be located.
        """
        return [r"C:\Windows\Fonts"]


class _Font(object):
    """
    A wrapper around an OTF/TTF font file stream that knows how to parse it
    for its name and style characteristics, e.g. bold and italic.
    """

    def __init__(self, stream):
        self._stream = stream

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_tb):
        self._stream.close()

    @property
    def is_bold(self):
        """
        |True| if this font is marked as a bold style of its font family.
        """
        try:
            return self._tables["head"].is_bold
        except KeyError:
            # some files don't have a head table
            return False

    @property
    def is_italic(self):
        """
        |True| if this font is marked as an italic style of its font family.
        """
        try:
            return self._tables["head"].is_italic
        except KeyError:
            # some files don't have a head table
            return False

    @classmethod
    def open(cls, font_file_path):
        """
        Return a |_Font| instance loaded from *font_file_path*.
        """
        return cls(_Stream.open(font_file_path))

    @property
    def family_name(self):
        """
        The name of the typeface family for this font, e.g. 'Arial'. The full
        typeface name includes optional style names, such as 'Regular' or
        'Bold Italic'. This attribute is only the common base name shared by
        all fonts in the family.
        """
        return self._tables["name"].family_name

    @lazyproperty
    def _fields(self):
        """5-tuple containing the fields read from the font file header.

        Also known as the offset table.
        """
        # sfnt_version, tbl_count, search_range, entry_selector, range_shift
        return self._stream.read_fields(">4sHHHH", 0)

    def _iter_table_records(self):
        """
        Generate a (tag, offset, length) 3-tuple for each of the tables in
        this font file.
        """
        count = self._table_count
        bufr = self._stream.read(offset=12, length=count * 16)
        tmpl = ">4sLLL"
        for i in range(count):
            offset = i * 16
            tag, checksum, off, len_ = unpack_from(tmpl, bufr, offset)
            yield tag.decode("utf-8"), off, len_

    @lazyproperty
    def _tables(self):
        """
        A mapping of OpenType table tag, e.g. 'name', to a table object
        providing access to the contents of that table.
        """
        return dict(
            (tag, _TableFactory(tag, self._stream, off, len_))
            for tag, off, len_ in self._iter_table_records()
        )

    @property
    def _table_count(self):
        """
        The number of tables in this OpenType font file.
        """
        return self._fields[1]


class _Stream(object):
    """A thin wrapper around a binary file that facilitates reading C-struct values."""

    def __init__(self, file):
        self._file = file

    @classmethod
    def open(cls, path):
        """Return |_Stream| providing binary access to contents of file at `path`."""
        return cls(open(path, "rb"))

    def close(self):
        """
        Close the wrapped file. Using the stream after closing raises an
        exception.
        """
        self._file.close()

    def read(self, offset, length):
        """
        Return *length* bytes from this stream starting at *offset*.
        """
        self._file.seek(offset)
        return self._file.read(length)

    def read_fields(self, template, offset=0):
        """
        Return a tuple containing the C-struct fields in this stream
        specified by *template* and starting at *offset*.
        """
        self._file.seek(offset)
        bufr = self._file.read(calcsize(template))
        return unpack_from(template, bufr)


class _BaseTable(object):
    """
    Base class for OpenType font file table objects.
    """

    def __init__(self, tag, stream, offset, length):
        self._tag = tag
        self._stream = stream
        self._offset = offset
        self._length = length


class _HeadTable(_BaseTable):
    """
    OpenType font table having the tag 'head' and containing certain header
    information for the font, including its bold and/or italic style.
    """

    def __init__(self, tag, stream, offset, length):
        super(_HeadTable, self).__init__(tag, stream, offset, length)

    @property
    def is_bold(self):
        """
        |True| if this font is marked as having emboldened characters.
        """
        return bool(self._macStyle & 1)

    @property
    def is_italic(self):
        """
        |True| if this font is marked as having italicized characters.
        """
        return bool(self._macStyle & 2)

    @lazyproperty
    def _fields(self):
        """
        A 17-tuple containing the fields in this table.
        """
        return self._stream.read_fields(">4s4sLLHHqqhhhhHHHHH", self._offset)

    @property
    def _macStyle(self):
        """
        The unsigned short value of the 'macStyle' field in this head table.
        """
        return self._fields[12]


class _NameTable(_BaseTable):
    """
    An OpenType font table having the tag 'name' and containing the
    name-related strings for the font.
    """

    def __init__(self, tag, stream, offset, length):
        super(_NameTable, self).__init__(tag, stream, offset, length)

    @property
    def family_name(self):
        """
        The name of the typeface family for this font, e.g. 'Arial'.
        """

        def find_first(dict_, keys, default=None):
            for key in keys:
                value = dict_.get(key)
                if value is not None:
                    return value
            return default

        # keys for Unicode, Mac, and Windows family name, respectively
        return find_first(self._names, ((0, 1), (1, 1), (3, 1)))

    @staticmethod
    def _decode_name(raw_name, platform_id, encoding_id):
        """
        Return the unicode name decoded from *raw_name* using the encoding
        implied by the combination of *platform_id* and *encoding_id*.
        """
        if platform_id == 1:
            # reject non-Roman Mac font names
            if encoding_id != 0:
                return None
            return raw_name.decode("mac-roman")
        elif platform_id in (0, 3):
            return raw_name.decode("utf-16-be")
        else:
            return None

    def _iter_names(self):
        """Generate a key/value pair for each name in this table.

        The key is a (platform_id, name_id) 2-tuple and the value is the unicode text
        corresponding to that key.
        """
        table_format, count, strings_offset = self._table_header
        table_bytes = self._table_bytes

        for idx in range(count):
            platform_id, name_id, name = self._read_name(
                table_bytes, idx, strings_offset
            )
            if name is None:
                continue
            yield ((platform_id, name_id), name)

    @staticmethod
    def _name_header(bufr, idx):
        """
        The (platform_id, encoding_id, language_id, name_id, length,
        name_str_offset) 6-tuple encoded in each name record C-struct.
        """
        name_hdr_offset = 6 + idx * 12
        return unpack_from(">HHHHHH", bufr, name_hdr_offset)

    @staticmethod
    def _raw_name_string(bufr, strings_offset, str_offset, length):
        """
        Return the *length* bytes comprising the encoded string in *bufr* at
        *str_offset* in the strings area beginning at *strings_offset*.
        """
        offset = strings_offset + str_offset
        tmpl = "%ds" % length
        return unpack_from(tmpl, bufr, offset)[0]

    def _read_name(self, bufr, idx, strings_offset):
        """Return a (platform_id, name_id, name) 3-tuple for name at `idx` in `bufr`.

        The triple looks like (0, 1, 'Arial'). `strings_offset` is the for the name at
        `idx` position in `bufr`. `strings_offset` is the index into `bufr` where actual
        name strings begin. The returned name is a unicode string.
        """
        platform_id, enc_id, lang_id, name_id, length, str_offset = self._name_header(
            bufr, idx
        )
        name = self._read_name_text(
            bufr, platform_id, enc_id, strings_offset, str_offset, length
        )
        return platform_id, name_id, name

    def _read_name_text(
        self, bufr, platform_id, encoding_id, strings_offset, name_str_offset, length
    ):
        """
        Return the unicode name string at *name_str_offset* or |None| if
        decoding its format is not supported.
        """
        raw_name = self._raw_name_string(bufr, strings_offset, name_str_offset, length)
        return self._decode_name(raw_name, platform_id, encoding_id)

    @lazyproperty
    def _table_bytes(self):
        """
        The binary contents of this name table.
        """
        return self._stream.read(self._offset, self._length)

    @property
    def _table_header(self):
        """
        The (table_format, name_count, strings_offset) 3-tuple contained
        in the header of this table.
        """
        return unpack_from(">HHH", self._table_bytes)

    @lazyproperty
    def _names(self):
        """A mapping of (platform_id, name_id) keys to string names for this font."""
        return dict(self._iter_names())


def _TableFactory(tag, stream, offset, length):
    """
    Return an instance of |Table| appropriate to *tag*, loaded from
    *font_file* with content of *length* starting at *offset*.
    """
    TableClass = {"head": _HeadTable, "name": _NameTable}.get(tag, _BaseTable)
    return TableClass(tag, stream, offset, length)
