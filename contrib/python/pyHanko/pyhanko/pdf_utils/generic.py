"""
Implementation of PDF object types and other generic functionality.
The internals were imported from PyPDF2, with modifications.

See :ref:`here <pypdf2-license>` for the original license
of the PyPDF2 project.
"""

import binascii
import codecs
import decimal
import enum
import logging
import os
import re
import typing
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any, Callable, Iterator, Optional, Tuple, Union

from .misc import (
    IndirectObjectExpected,
    PdfError,
    PdfReadError,
    PdfStreamError,
    PdfStrictReadError,
    PdfWriteError,
    is_regular_character,
    read_non_whitespace,
    read_until_delimiter,
    read_until_regex,
    skip_over_whitespace,
)

if typing.TYPE_CHECKING:
    from .crypt.api import SecurityHandler

__all__ = [
    'Dereferenceable',
    'Reference',
    'TrailerReference',
    'PdfObject',
    'IndirectObject',
    'NullObject',
    'BooleanObject',
    'FloatObject',
    'NumberObject',
    'ByteStringObject',
    'TextStringObject',
    'NameObject',
    'ArrayObject',
    'DictionaryObject',
    'StreamObject',
    'read_object',
    'pdf_name',
    'pdf_string',
    'pdf_date',
    'TextStringEncoding',
    'EncryptedObjAccess',
    'DecryptedObjectProxy',
]

OBJECT_PREFIXES = b'/<[tf(n%'
NUMBER_SIGNS = b'+-'
INDIRECT_PATTERN = re.compile(r"(\d+)\s+(\d+)\s+R[^a-zA-Z]".encode('ascii'))

logger = logging.getLogger(__name__)


class EncryptedObjAccess(enum.Enum):
    """
    Defines what to do when an encrypted object is encountered when retrieving
    an object from a container.
    """

    PROXY = 0
    """
    Return the proxy object as-is, and leave further encryption/decryption
    handling to the caller.
    """

    TRANSPARENT = 1
    """
    Transparently decrypt the proxy's content (similarly wrapping any
    sub-containers in :class:`.DecryptedObjectProxy`, so this applies
    recursively).

    .. note::
        This is the default in most situations, since it's the least likely
        to get in the way of any APIs that are not explicitly aware of
        content encryption concerns.
    """

    RAW = 2
    """
    Return the underlying raw object as written, without attempting or deferring
    decryption.
    """


def _deproxy_decrypt(obj, eoa: EncryptedObjAccess):
    if isinstance(obj, DecryptedObjectProxy):
        if eoa == EncryptedObjAccess.TRANSPARENT:
            return obj.decrypted
        elif eoa == EncryptedObjAccess.RAW:
            return obj.raw_object
    return obj


class Dereferenceable:
    """
    Represents an opaque reference to a PDF object associated with
    a PDF Handler (see :class:`PdfHandler <.rw_common.PdfHandler>`).

    This can either be a reference to an object with an object ID
    (see :class:`.Reference`) or a reference to the trailer of a PDF document
    (see :class:`.TrailerReference`).
    """

    def get_object(self) -> 'PdfObject':
        """Retrieve the PDF object backing this dereferenceable.

        :return: A :class:`.PdfObject`.
        """
        raise NotImplementedError

    def get_pdf_handler(self):
        """Return the PDF handler associated with this dereferenceable.

        :return: a :class:`~.rw_common.PdfHandler`.
        """
        raise NotImplementedError


class TrailerReference(Dereferenceable):
    """
    A reference to the trailer of a PDF document.

    .. warning::
       Since the trailer does not have a well-defined object ID in files with
       "classical" cross-reference tables (as opposed to cross-reference
       streams), this is not a subclass of :class:`.Reference`.

    :param reader:
        a :class:`~pyhanko.pdf_utils.reader.PdfFileReader`
    """

    def __init__(self, reader):
        self.reader = reader

    def get_object(self) -> 'PdfObject':
        return self.reader.trailer

    def get_pdf_handler(self):
        return self.reader


@dataclass(frozen=True)
class Reference(Dereferenceable):
    """
    A reference to an object with a certain ID and generation number, with
    a PDF handler attached to it.

    .. warning::
       Contrary to what one might expect, the generation number does *not*
       indicate the document revision in which the object was modified. In fact,
       nonzero generation numbers are exceedingly rare these days; in most
       real-world PDF files, objects are simply overridden without ever
       increasing the generation number.

       Except in very specific circumstances, dereferencing a
       :class:`.Reference` will return the most recent version of the object
       with the stated object ID and generation number.
    """

    idnum: int
    """
    The object's ID.
    """

    generation: int = 0
    """
    The object's generation number (usually `0`)
    """

    pdf: object = field(repr=False, hash=False, compare=False, default=None)
    """
    The PDF handler associated with this reference, an instance of
    :class:`~.rw_common.PdfHandler`.
    
    .. warning::
       This field is ignored when hashing or comparing :class:`.Reference`
       objects, so it is the API user's responsibility to not mix up
       references originating from unrelated PDF handlers.
    """

    def get_object(self) -> 'PdfObject':
        if self.pdf is None:
            return NullObject()
        from pyhanko.pdf_utils.rw_common import PdfHandler

        assert isinstance(self.pdf, PdfHandler)
        return self.pdf.get_object(self).get_object()

    def get_pdf_handler(self):
        return self.pdf


def read_object(
    stream, container_ref: 'Dereferenceable', as_metadata_stream: bool = False
) -> 'PdfObject':
    """
    Read a PDF object from an input stream.

    .. note::
       The `container_ref` parameter tells the API which reference to register
       when the returned object is modified in an incremental update.
       See also here :ref:`here <container-ref-example>` for further
       information.

    :param stream:
        An input stream.
    :param container_ref:
        A reference to an object containing this one.

        *Note:* It is perfectly possible (and common) for `container_ref` to
        resolve to the return value of this function.
    :param as_metadata_stream:
        Whether to dereference the object as an XMP metadata stream.
    :return:
        A :class:`.PdfObject`.
    """

    tok = stream.read(1)
    stream.seek(-1, os.SEEK_CUR)  # reset to start
    idx = OBJECT_PREFIXES.find(tok)
    if idx == 0:
        # name object
        result = NameObject.read_from_stream(stream)
    elif idx == 1:
        # hexadecimal string OR dictionary
        peek = stream.read(2)
        stream.seek(-2, os.SEEK_CUR)  # reset to start
        if peek == b'<<':
            result = DictionaryObject.read_from_stream(
                stream, container_ref, as_metadata_stream=as_metadata_stream
            )
        else:
            result = read_hex_string_from_stream(stream)
    elif idx == 2:
        # array object
        result = ArrayObject.read_from_stream(stream, container_ref)
    elif idx == 3 or idx == 4:
        # boolean object
        result = BooleanObject.read_from_stream(stream)
    elif idx == 5:
        # string object
        result = read_string_from_stream(stream)
    elif idx == 6:
        # null object
        result = NullObject.read_from_stream(stream)
    elif idx == 7:
        # comment
        while tok not in (b'\r', b'\n'):
            tok = stream.read(1)
        read_non_whitespace(stream)
        stream.seek(-1, os.SEEK_CUR)
        result = read_object(stream, container_ref)
    else:
        # number object OR indirect reference
        if tok in NUMBER_SIGNS:
            # number
            result = NumberObject.read_from_stream(stream)
        else:
            peek = stream.read(20)
            stream.seek(-len(peek), os.SEEK_CUR)  # reset to start
            if INDIRECT_PATTERN.match(peek) is not None:
                result = IndirectObject.read_from_stream(stream, container_ref)
            else:
                result = NumberObject.read_from_stream(stream)

    result.container_ref = container_ref
    return result


class PdfObject:
    """Superclass for all PDF objects."""

    container_ref: Optional[Dereferenceable] = None
    """
    For objects read from a file, `container_ref` points to the unique
    addressable object containing this object.
    
    .. _container-ref-example:
    
    .. note::
        Consider the following object definition in a PDF file:
        
        .. code-block:: text
        
           4 0 obj
           << /Foo (Bar) >>
          
        This declares a dictionary with ID `4`, but the values ``/Foo`` and 
        ``(Bar)`` are also PDF objects (a name and a string, respectively).
        All of these will have `container_ref` given by a :class:`.Reference`
        with object ID `4` and generation number `0`.
    
    If an object is part of the trailer of a PDF file, `container_ref` will be
    a :class:`.TrailerReference`.
    For newly created objects (i.e. those not read from a file), `container_ref`
    is always ``None``.
    """

    # TODO simplify a number of modification routines using this new API
    def get_container_ref(self) -> Dereferenceable:
        """
        Return a reference to the closest parent object containing this object.
        Raises an error if no such reference can be found.
        """
        ref = self.container_ref
        if ref is None:  # pragma: nocover
            raise PdfReadError(
                'No container reference available. This object probably '
                'wasn\'t read from a file.'
            )
        return ref

    def get_object(self):
        """Resolves indirect references.

        :return: `self`, unless an instance of :class:`.IndirectObject`.
        """
        return self

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref: Optional[Reference] = None,
    ):
        """
        Abstract method to render this object to an output stream.

        :param stream:
            An output stream.
        :param container_ref:
            Local encryption key.
        :param handler:
            Security handler
        """
        raise NotImplementedError


class NullObject(PdfObject):
    """
    PDF `null` object.

    All instances are treated as equal and falsy.
    """

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        stream.write(b"null")

    @staticmethod
    def read_from_stream(stream):
        nulltxt = stream.read(4)
        if nulltxt != b"null":
            raise PdfReadError("Could not read Null object")
        return NullObject()

    def __eq__(self, other):
        return self is other or isinstance(other, NullObject)

    def __hash__(self):
        return hash(None)

    def __bool__(self):
        return False


class BooleanObject(PdfObject):
    """PDF boolean value."""

    def __init__(self, value):
        self.value = value

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        if self.value:
            stream.write(b"true")
        else:
            stream.write(b"false")

    @staticmethod
    def read_from_stream(stream):
        word = stream.read(4)
        if word == b"true":
            return BooleanObject(True)
        elif word == b"fals":
            if stream.read(1) == b"e":
                return BooleanObject(False)
        raise PdfReadError('Could not read Boolean object')

    def __bool__(self):
        return bool(self.value)

    def __eq__(self, other):
        return isinstance(other, (BooleanObject, bool)) and bool(self) == bool(
            other
        )

    def __str__(self):
        return str(bool(self))

    def __repr__(self):
        return str(self)


class ArrayObject(list, PdfObject):
    """
    PDF array object. This class extends from Python's list class,
    and supports its interface.

    .. warning::
        Contrary to the case of dictionary objects, PyPDF2 does not
        transparently dereference array entries when accessed using
        :meth:`__getitem__`.
        For usability & consistency reasons, I decided to depart from that
        and dereference automatically.
        This makes the behaviour of :class:`.ArrayObject` consistent with
        :class:`.DictionaryObject`.

        That said, some vestiges of the old PyPDF2 behaviour may linger in
        the codebase. I'll fix those as I get to them.
    """

    def __getitem__(self, index):
        return self.raw_get(index).get_object()

    def raw_get(
        self,
        index,
        decrypt: EncryptedObjAccess = EncryptedObjAccess.TRANSPARENT,
    ):
        """
        .. versionchanged:: 0.14.0

            ``decrypt`` parameter is no longer boolean

        Get a value from an array without dereferencing.
        In other words, if the value corresponding to the given key is of type
        :class:`.IndirectObject`, the indirect reference will not be resolved.

        :param index:
            Key to look up in the dictionary.
        :param decrypt:
            What to do when retrieving encrypted objects; see
            :class:`.EncryptedObjAccess`. The default is
            :attr:`.EncryptedObjAccess.TRANSPARENT`.
        :return:
            A :class:`.PdfObject`.
        """

        val = list.__getitem__(self, index)
        return _deproxy_decrypt(val, decrypt)

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        stream.write(b"[")
        for data in self:
            stream.write(b" ")
            data.write_to_stream(
                stream, handler=handler, container_ref=container_ref
            )
        stream.write(b" ]")

    @staticmethod
    def read_from_stream(stream, container_ref):
        arr = ArrayObject()
        tmp = stream.read(1)
        if tmp != b"[":
            raise PdfReadError("Could not read array")
        while True:
            # skip leading whitespace & check for array ending
            peekahead = read_non_whitespace(stream)
            if peekahead == b"]":
                break
            stream.seek(-1, os.SEEK_CUR)
            # read and append obj
            arr.append(read_object(stream, container_ref))
        return arr


class IndirectObject(PdfObject, Dereferenceable):
    """
    Thin wrapper around a :class:`.Reference`, implementing both the
    :class:`.Dereferenceable` and :class:`.PdfObject` interfaces.

    .. warning::
        For many purposes, this class is functionally interchangeable with
        :class:`.Reference`, with one important exception:
        :class:`.IndirectObject` instances pointing to the same reference
        but occurring at different locations in the file may have distinct
        `container_ref` values.
    """

    def __init__(self, idnum, generation, pdf):
        self.reference = Reference(idnum, generation, pdf)

    def get_object(self):
        """
        :return: The PDF object this reference points to.
        """
        obj = self.reference.get_object()
        # there are few legitimate use cases for indirect references
        #  pointing to indirect references, but the standard doesn't forbid
        #  them, so we have to support them.
        # TODO protect against reference loops?
        return obj.get_object() if isinstance(obj, IndirectObject) else obj

    def get_pdf_handler(self):
        return self.reference.get_pdf_handler()

    @property
    def idnum(self) -> int:
        """
        :return: the object ID of this reference.
        """
        return self.reference.idnum

    @property
    def generation(self):
        """
        :return: the generation number of this reference.
        """
        return self.reference.generation

    def __repr__(self):
        return "IndirectObject(%r, %r)" % (self.idnum, self.generation)

    # TODO I'm starting to think that making indirect objects hashable
    #  is a bad idea. Think about that for a bit, I might just be getting
    #  overly pedantic.
    def __hash__(self):
        return hash((self.idnum, self.generation))

    def __eq__(self, other):
        return (
            other is not None
            and isinstance(other, IndirectObject)
            and self.reference == other.reference
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        stream.write(b"%d %d R" % (self.idnum, self.generation))

    @staticmethod
    def read_from_stream(stream, container_ref: 'Dereferenceable'):
        idnum_str = b""
        while True:
            tok = stream.read(1)
            if not tok:
                # stream has truncated prematurely
                raise PdfStreamError("Stream has ended unexpectedly")
            if tok.isspace():
                if not idnum_str:
                    continue
                break
            idnum_str += tok
        generation_str = b""
        while True:
            tok = stream.read(1)
            if not tok:
                # stream has truncated prematurely
                raise PdfStreamError("Stream has ended unexpectedly")
            if tok.isspace():
                if not generation_str:
                    continue
                break
            generation_str += tok
        r = read_non_whitespace(stream)
        if r != b"R":
            pos = hex(stream.tell())
            raise PdfReadError(
                "Error reading indirect object reference at byte %s" % pos
            )
        try:
            idnum, generation = int(idnum_str), int(generation_str)
            if not (idnum > 0 and generation >= 0):
                raise ValueError
        except ValueError:
            pos = hex(stream.tell())
            raise PdfReadError(
                f"Parse error on indirect object reference around {pos}"
            )
        return IndirectObject(
            int(idnum_str), int(generation_str), container_ref.get_pdf_handler()
        )


class FloatObject(decimal.Decimal, PdfObject):
    """
    PDF Float object.

    Internally, these are treated as decimals (and therefore actually
    fixed-point objects, to be precise).
    """

    def __new__(cls, value="0"):
        return decimal.Decimal.__new__(cls, str(value))

    def __repr__(self):
        if self == self.to_integral():
            return str(self.quantize(decimal.Decimal(1)))
        else:
            return str(self)

    def as_numeric(self):
        """
        :return: a Python ``float`` value for this object.
        """
        return float(self)

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        stream.write(repr(self).encode('ascii'))


class NumberObject(int, PdfObject):
    """
    PDF number object. This is the PDF type for integer values.
    """

    NumberPattern = re.compile(b'[^+-.0-9]')
    ByteDot = b"."

    # noinspection PyArgumentList
    def __new__(cls, value):
        val = int(value)
        return int.__new__(cls, val)

    def as_numeric(self):
        """
        :return: a Python ``int`` value for this object.
        """
        return int(self)

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        stream.write(repr(self).encode('ascii'))

    @staticmethod
    def read_from_stream(stream):
        num = read_until_regex(
            stream,
            regex=NumberObject.NumberPattern,
            # for consistency with other read_object() output
            ignore_eof=True,
        )
        if num.find(NumberObject.ByteDot) != -1:
            return FloatObject(num.decode('ascii'))
        else:
            return NumberObject(num.decode('ascii'))


# TODO: not sure I like this behaviour of PyPDF2. Review.


def pdf_string(
    string: Union[str, bytes, bytearray]
) -> Union['ByteStringObject', 'TextStringObject']:
    """
    Encode a string as a :class:`.TextStringObject` if possible,
    or a :class:`.ByteStringObject` otherwise.

    :param string:
        A Python string.
    """
    if isinstance(string, str):
        return TextStringObject(string)
    elif isinstance(string, (bytes, bytearray)):
        guessed = _guess_enc_by_bom(string)
        try:
            retval = TextStringObject(guessed.decode(string))
            retval.autodetected_encoding = guessed
            return retval
        except UnicodeDecodeError:
            return ByteStringObject(string)
    else:
        raise TypeError("pdf_string should have str or bytes arg")


HEX_DIGITS = b'0123456789abcdefABCDEF'


def read_hex_string_from_stream(
    stream,
) -> Union['ByteStringObject', 'TextStringObject']:
    """
    Read a hex string from a stream into a PDF string object.

    :param stream:
        An input stream.
    """
    stream.read(1)

    odd = False

    def read_tokens():
        nonlocal odd
        while True:
            tok = read_non_whitespace(stream)
            if tok == b">":
                return
            elif tok not in HEX_DIGITS:
                raise PdfStreamError(
                    "Unexpected token in hex string: " + repr(tok)
                )
            yield tok
            odd = not odd

    result = binascii.unhexlify(
        b''.join(read_tokens()) + (b'0' if odd else b'')
    )
    return pdf_string(result)


def _read_string_literal_bytes(stream) -> bytes:
    stream.read(1)
    parens = 1
    txt = BytesIO()
    while True:
        tok = stream.read(1)
        if not tok:
            # stream has truncated prematurely
            raise PdfStreamError("Stream has ended unexpectedly")
        if tok == b"(":
            parens += 1
        elif tok == b")":
            parens -= 1
            if parens == 0:
                break
        elif tok == b"\\":
            tok = stream.read(1)
            if tok in b"() /%<>[]#_&$\\":
                pass  # simply use the second byte we read
            elif tok == b"n":
                tok = b"\n"
            elif tok == b"r":
                tok = b"\r"
            elif tok == b"t":
                tok = b"\t"
            elif tok == b"b":
                tok = b"\b"
            elif tok == b"f":
                tok = b"\f"
            elif tok.isdigit():
                # "The number ddd may consist of one, two, or three
                # octal digits; high-order overflow shall be ignored.
                # Three octal digits shall be used, with leading zeros
                # as needed, if the next character of the string is also
                # a digit." (PDF reference 7.3.4.2, p 16)
                for i in range(2):
                    ntok = stream.read(1)
                    if ntok.isdigit():
                        tok += ntok
                    else:
                        # premature end, seek back
                        stream.seek(-1, os.SEEK_CUR)
                        break
                octal = int(tok, base=8)
                # interpret as byte
                tok = bytes((octal,))
            elif tok in b"\n\r":
                # This case is  hit when a backslash followed by a line
                # break occurs.  If it's a multi-char EOL, consume the
                # second character:
                tok = stream.read(1)
                if tok not in b"\n\r":
                    stream.seek(-1, os.SEEK_CUR)
                # Then don't add anything to the actual string, since this
                # line break was escaped:
                tok = b''
            else:
                raise PdfReadError("Unexpected escaped string: " + repr(tok))
        txt.write(tok)
    return txt.getvalue()


def read_string_from_stream(
    stream,
) -> Union['ByteStringObject', 'TextStringObject']:
    """
    Read a PDF string literal from a stream. Attempt to decode it into a text
    string by autodetecting the encoding, or failing that, return it as a byte
    string instead.

    :param stream:
        An input stream.
    """

    return pdf_string(_read_string_literal_bytes(stream))


class ByteStringObject(bytes, PdfObject):
    """PDF bytestring class."""

    original_bytes = property(lambda self: self)
    """
    For compatibility with :attr:`.TextStringObject.original_bytes`
    """

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        bytearr: bytes = self
        if handler is not None and container_ref is not None:
            cf = handler.get_string_filter()
            local_key = cf.derive_object_key(
                container_ref.idnum, container_ref.generation
            )
            bytearr = cf.encrypt(local_key, bytearr)
        stream.write(b"<")
        stream.write(binascii.hexlify(bytearr))
        stream.write(b">")


class TextStringEncoding(enum.Enum):
    """
    Encodings for PDF text strings.
    """

    PDF_DOC = None
    """
    PDFDocEncoding (one-byte character codes; PDF-specific).
    """

    UTF16BE = (codecs.BOM_UTF16_BE, 'utf-16be')
    """
    UTF-16BE encoding.
    """

    UTF8 = (codecs.BOM_UTF8, 'utf-8')
    """
    UTF-8 encoding (PDF 2.0)
    """

    UTF16LE = (codecs.BOM_UTF16_LE, 'utf-16le')
    """
    UTF-16LE encoding.

    .. note::
        This is strictly speaking invalid in PDF 2.0, but some authoring tools
        output such strings anyway (presumably due to the fact that it's the
        default wide character encoding on Windows).
    """

    def encode(self, string: str) -> bytes:
        """
        Encode a string with BOM.

        :param string:
            The string to encode.
        :return:
            The encoded string.
        """
        if self == TextStringEncoding.PDF_DOC:
            return encode_pdfdocencoding(string)
        else:
            bom, enc = self.value
            return bom + string.encode(enc)

    def decode(self, string: Union[bytes, bytearray]) -> str:
        """
        Decode a string with BOM.

        :param string:
            The string to encode.
        :return:
            The encoded string.
        :raise UnicodeDecodeError:
            Raised if decoding fails.
        """
        if self == TextStringEncoding.PDF_DOC:
            return decode_pdfdocencoding(string)
        elif self == TextStringEncoding.UTF8:
            return string.decode('utf-8-sig')
        else:
            return string.decode('utf-16')


def _guess_enc_by_bom(encoded: Union[bytes, bytearray]) -> TextStringEncoding:
    if encoded.startswith(codecs.BOM_UTF16_BE):
        return TextStringEncoding.UTF16BE
    elif encoded.startswith(codecs.BOM_UTF16_LE):
        return TextStringEncoding.UTF16LE
    elif encoded.startswith(codecs.BOM_UTF8):
        return TextStringEncoding.UTF8
    else:
        # This is probably a big performance hit here, but we need to
        # convert string objects into the text/unicode-aware version if
        # possible... and the only way to check if that's possible is
        # to try.  Some strings are strings, some are just byte arrays.
        return TextStringEncoding.PDF_DOC


class TextStringObject(str, PdfObject):
    """
    PDF text string object.
    """

    autodetected_encoding: Optional[TextStringEncoding] = None
    """
    Autodetected encoding when parsing the file.
    """

    force_output_encoding: Optional[TextStringEncoding] = None
    """
    Output encoding to use when serialising the string.
    The default is to try PDFDocEncoding first, and fall back to UTF-16BE.
    """

    @property
    def original_bytes(self):
        """
        Retrieve the original bytes of the string as specified in the
        source file.

        This may be necessary if this string was misidentified as a text string.
        """

        # We're a text string object, but the library is trying to get our raw
        # bytes.  This can happen if we auto-detected this string as text, but
        # we were wrong.  It's pretty common.  Return the original bytes that
        # would have been used to create this object, based upon the autodetect
        # method.
        if self.autodetected_encoding:
            return self.autodetected_encoding.encode(self)
        else:
            raise PdfError("No information about original bytes")

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        encoded: bytes
        if self.force_output_encoding is not None:
            encoded = self.force_output_encoding.encode(self)
        else:
            # Try to write the string out as a PDFDocEncoding encoded string.
            # It's nicer to look at in the PDF file.  Sadly, we take a
            # performance hit here for trying...
            try:
                encoded = encode_pdfdocencoding(self)
            except UnicodeEncodeError:
                # fall back to UTF-16BE by default, since it's the only
                # valid pre-2.0 Unicode encoding.
                encoded = codecs.BOM_UTF16_BE + self.encode("utf-16be")

        cf = None
        if handler is not None and container_ref is not None:
            cf_name = handler.crypt_filter_config.string_filter_name
            # apply default processing if the filter is the identity filter
            cf = None if cf_name == '/Identity' else handler.get_string_filter()

        if cf is not None:
            local_key = cf.derive_object_key(
                container_ref.idnum, container_ref.generation
            )
            encoded = cf.encrypt(local_key, encoded)
            obj = ByteStringObject(encoded)
            obj.write_to_stream(stream)
        else:
            stream.write(b"(")
            for c in encoded:
                c_ = bytes([c])
                if not c_.isalnum() and c != 0x20:
                    stream.write(b"\\%03o" % c)
                else:
                    stream.write(c_)
            stream.write(b")")


def _as_hex_digit(ascii_char):
    if 0x30 <= ascii_char <= 0x39:
        return ascii_char - 0x30
    elif 0x41 <= ascii_char <= 0x46:
        return ascii_char - 0x37
    elif 0x61 <= ascii_char <= 0x66:
        return ascii_char - 0x57
    else:
        raise PdfReadError(
            "Numeric escape in PDF name must use hexadecimal digits"
        )


def _decode_name(name_bytes: bytes) -> 'NameObject':
    """
    Decode the bytes that make up a name object (minus the initial /), expanding
    all escapes along the way.
    """
    result = BytesIO()
    result.write(b'/')
    name_iter = iter(name_bytes)
    try:
        while True:
            cur_byte = next(name_iter)
            if cur_byte == 0x23:  # '#' is the 2-digit escape prefix
                # escape sequence: grab next two bytes
                try:
                    digit1 = next(name_iter)
                    digit2 = next(name_iter)
                except StopIteration:
                    raise PdfReadError(
                        f"Unterminated escape in PDF name /{repr(name_bytes)}"
                    )

                cur_byte = _as_hex_digit(digit1) * 16 + _as_hex_digit(digit2)
            elif not (0x21 <= cur_byte <= 0x7E) or not is_regular_character(
                cur_byte
            ):
                raise PdfReadError(
                    f"Byte (0x{cur_byte:02x}) must be escaped in a PDF name"
                )
            result.write(bytes((cur_byte,)))
    except StopIteration:
        pass
    name_bytes = result.getvalue()
    # NOTE: we assume UTF-8, but the PDF spec actually doesn't prescribe
    #  a character encoding for names, they're just byte sequences.
    #  This doesn't matter in 99.99% of cases (since names are not supposed
    #  to contain renderable text, and are typically 7-bit ASCII anyhow),
    #  but it's not 100% correct. I don't see a way to fix this without causing
    #  massive non-obvious API breakage (since NameObject inherits from 'str' as
    #  in PyPDF2), i.e. the correctness benefit is vastly outweighed by the
    #  risks (for now)
    encodings_to_try = ('utf8', 'latin1')
    # latin1 should never trigger decoding errors, since Python's implementation
    # maps even unassigned values to corresponding unicode codepoints
    name_str = None
    for enc in encodings_to_try:
        try:
            name_str = name_bytes.decode(enc)
            break
        except ValueError:
            pass
    assert name_str is not None
    return NameObject(name_str)


class NameObject(str, PdfObject):
    """
    PDF name object. These are valid Python strings, but names and strings
    are treated differently in the PDF specification, so proper care is
    required.
    """

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        byte_iter = iter(self.encode('utf8'))
        if not next(byte_iter) == 0x2F:
            raise PdfWriteError(
                f"Could not serialise name object {repr(self)}, "
                f"must start with /"
            )
        stream.write(b'/')
        for cur_byte in byte_iter:
            if (
                cur_byte == 0x23
                or not (0x21 <= cur_byte <= 0x7E)
                or not is_regular_character(cur_byte)
            ):
                stream.write('#{:X}'.format(cur_byte).encode('ascii'))
            else:
                # no convenient syntax for writing a single byte...
                as_bytes = bytes((cur_byte,))
                stream.write(as_bytes)

    @staticmethod
    def read_from_stream(stream):
        name_start = stream.read(1)
        if name_start != b'/':
            raise PdfReadError("Name object should start with /")
        name_bytes = read_until_delimiter(stream)
        return _decode_name(name_bytes)


def _normalise_key(key):
    if not isinstance(key, NameObject):
        if isinstance(key, str):
            return NameObject(key)
        else:
            raise ValueError("key must be a name object")
    return key


class DictionaryObject(dict, PdfObject):
    """
    A PDF dictionary object.

    Keys in a PDF dictionary are PDF names, and values are PDF objects.

    When accessing a key using the standard :meth:`__getitem__` syntax,
    :class:`.IndirectObject` references will be resolved.
    """

    def __init__(self, dict_data=None):
        if dict_data is not None:
            super().__init__(
                {_normalise_key(k): v for k, v in dict_data.items()}
            )
        else:
            super().__init__()

    def raw_get(
        self,
        key: Union[NameObject, str],
        decrypt: EncryptedObjAccess = EncryptedObjAccess.TRANSPARENT,
    ):
        """
        .. versionchanged:: 0.14.0

            ``decrypt`` parameter is no longer boolean

        Get a value from a dictionary without dereferencing.
        In other words, if the value corresponding to the given key is of type
        :class:`.IndirectObject`, the indirect reference will not be resolved.

        :param key:
            Key to look up in the dictionary.
        :param decrypt:
            What to do when retrieving encrypted objects; see
            :class:`.EncryptedObjAccess`. The default is
            :attr:`.EncryptedObjAccess.TRANSPARENT`.
        :return:
            A :class:`.PdfObject`.
        """
        val = dict.__getitem__(self, key)
        return _deproxy_decrypt(val, decrypt)

    def __setitem__(self, key, value):
        key = _normalise_key(key)
        if not isinstance(value, PdfObject):
            raise ValueError("value must be PdfObject")
        if self.container_ref is not None:
            value.container_ref = self.container_ref
        return dict.__setitem__(self, key, value)

    def setdefault(self, key, value=None):
        key = _normalise_key(key)
        if not isinstance(value, PdfObject):
            raise ValueError("value must be PdfObject")
        if self.container_ref is not None:
            value.container_ref = self.container_ref
        return dict.setdefault(self, key, value)

    def __getitem__(self, key):
        raw_obj = dict.__getitem__(self, key)
        if key == '/Metadata' and isinstance(raw_obj, IndirectObject):
            from pyhanko.pdf_utils.rw_common import PdfHandler

            handler = raw_obj.get_pdf_handler()
            assert isinstance(handler, PdfHandler)
            return handler.get_object(
                raw_obj.reference, as_metadata_stream=True
            )
        else:
            deref_obj = raw_obj.get_object()
            if isinstance(deref_obj, NullObject):
                raise KeyError(key)
            else:
                return deref_obj

    def get_and_apply(
        self,
        key,
        function: Callable[[PdfObject], Any],
        *,
        raw=False,
        default=None,
    ):
        try:
            value = self.raw_get(key) if raw else self[key]
        except KeyError:
            return default
        return function(value)

    def get_value_as_reference(self, key, optional=False) -> Reference:
        def as_ref(obj):
            if isinstance(obj, IndirectObject):
                return obj.reference
            raise IndirectObjectExpected

        value = self.get_and_apply(key, as_ref, raw=True)
        if value is None and not optional:
            raise KeyError
        return value

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        stream.write(b"<<\n")
        for key, value in list(self.items()):
            key.write_to_stream(stream, handler, container_ref)
            stream.write(b" ")
            value.write_to_stream(stream, handler, container_ref)
            stream.write(b"\n")
        stream.write(b">>")

    @staticmethod
    def read_from_stream(
        stream,
        container_ref: 'Dereferenceable',
        as_metadata_stream: bool = False,
    ):
        tmp = stream.read(2)
        if tmp != b"<<":
            raise PdfReadError(
                "Dictionary read error at byte 0x%s: "
                "stream must begin with '<<'" % hex(stream.tell())
            )
        data = {}
        handler = container_ref.get_pdf_handler()
        while True:
            tok = read_non_whitespace(stream)
            if tok == b">":
                stream.read(1)
                break
            stream.seek(-1, os.SEEK_CUR)
            try:
                key = NameObject.read_from_stream(stream)
            except Exception as ex:
                raise PdfReadError(
                    "Failed to read dictionary key at byte 0x%s; expected PDF name"
                    % hex(stream.tell())
                ) from ex
            read_non_whitespace(stream)
            stream.seek(-1, os.SEEK_CUR)
            value = read_object(stream, container_ref)
            if key not in data:
                data[key] = value
            else:
                err = (
                    "Multiple definitions in dictionary at byte "
                    "%s for key %s" % (hex(stream.tell()), key)
                )
                if handler.strict:
                    raise PdfStrictReadError(err)
                else:
                    logger.warning(err)

        pos = stream.tell()
        s = read_non_whitespace(stream, allow_eof=True)
        stream_data = None
        if s == b's' and stream.read(5) == b'tream':
            # odd PDF file output has spaces after 'stream' keyword
            # but before EOL. Original PyPDF2 patch provided by Danial Sandler,
            # modified by Matthias Valvekens
            skip_over_whitespace(stream, stop_after_eol=True)
            # this is a stream object, not a dictionary
            length = data[pdf_name("/Length")]
            if isinstance(length, IndirectObject):
                t = stream.tell()
                length = handler.get_object(length)
                stream.seek(t)
            stream_data = stream.read(length)
            e = read_non_whitespace(stream)
            ndstream = stream.read(8)
            if (e + ndstream) != b"endstream":
                # (sigh) - the odd PDF file has a length that is too long, so
                # we need to read backwards to find the "endstream" ending.
                # ReportLab (unknown version) generates files with this bug,
                # and Python users into PDF files tend to be our audience.
                # we need to do this to correct the streamdata and chop off
                # an extra character.
                orig_endstream_pos = stream.tell()
                stream.seek(-10, os.SEEK_CUR)
                end = stream.read(9)
                if end == b"endstream":
                    # we found it by looking back one character further.
                    stream_data = stream_data[:-1]
                else:
                    raise PdfReadError(
                        "Unable to find 'endstream' marker after "
                        "stream at byte %s." % hex(orig_endstream_pos)
                    )
        else:
            stream.seek(pos)
        if stream_data is not None:
            # pass in everything as encoded data, the StreamObject class
            # will take care of decoding as necessary
            stm_cls = StreamObject
            if as_metadata_stream:
                try:
                    # noinspection PyUnresolvedReferences
                    from pyhanko.pdf_utils.metadata.xmp_xml import (
                        MetadataStream,
                    )

                    stm_cls = MetadataStream
                except ImportError:  # pragma: nocover
                    pass
            return stm_cls(data, encoded_data=stream_data)
        else:
            return DictionaryObject(data)


class StreamObject(DictionaryObject):
    """
    PDF stream object.

    Essentially, a PDF stream is a dictionary object with a binary blob of
    data attached. This data can be encoded by various filters (not all of which
    are currently supported, see :mod:`.filters`).

    A stream object can be initialised with encoded or decoded data.
    The former is used by :class:`.reader.PdfFileReader` to provide on-demand
    decoding, with :class:`.writer.BasePdfFileWriter` and its subclasses working
    the other way around.

    .. note::
        The :class:`.StreamObject` class manages some of its dictionary
        keys by itself. This is partly the case for the various ``/Filter``
        and ``/DecodeParms`` entries, but also for the ``/Length`` entry.
        The latter will be overwritten as necessary.

    :param dict_data:
        The dictionary data for this stream object.
    :param stream_data:
        The (unencoded) stream data.
    :param encoded_data:
        The encoded stream data.

        .. warning::
            Ordinarily, a stream can be initialised either from decoded and from
            encoded data.

            If both `stream_data` and `encoded_data` are provided, the caller
            is responsible for making sure that both are compatible given the
            currently relevant filter configuration.
    :param handler:
        A reference to the currently active
        :class:`.pyhanko.pdf_utils.crypt.SecurityHandler`.
        This is only necessary if the stream requires crypt filters.
    """

    def __init__(
        self,
        dict_data: Optional[dict] = None,
        stream_data: Optional[bytes] = None,
        encoded_data: Optional[bytes] = None,
        handler: Optional['SecurityHandler'] = None,
    ):
        super().__init__(dict_data)
        self._data = stream_data
        self._encoded_data = encoded_data
        self._handler = handler

    def _implicit_decrypt_stream_content(
        self, handler, ref: Reference, decrypted_entries: dict
    ):
        """
        Internal method to handle decrypting streams that are encrypted
        with the document's default encryption handler for streams and/or
        embedded files (i.e. not with any custom crypt filters).

        This routine is called deep in the object fetching stack, and you should
        never invoke it yourself. It's defined as a method in
        :class:`.StreamObject` because it needs to be able to preserve the
        type (subclass) of the stream object on which it is called, in order
        to properly feed into the logic surrounding metadata streams.
        """

        if handler is not None:
            self._handler = handler
        # can't deal with crypt filters here
        if self._has_crypt_filter:
            # in this case, dealing with encryption is delegated
            # to the stream decoding process, so just pretend the data
            # is decrypted.
            # We pass a reference to the security handler below,
            # which is sufficient to take care of /Crypt filters
            # in the stream.
            decrypted_data = self.encoded_data
        else:
            if self.is_embedded_file_stream:
                cf = handler.get_embedded_file_filter()
            else:
                cf = handler.get_stream_filter()
            local_key = cf.derive_object_key(ref.idnum, ref.generation)
            decrypted_data = cf.decrypt(local_key, self.encoded_data)

        return self.__class__(
            decrypted_entries, encoded_data=decrypted_data, handler=handler
        )

    @property
    def _has_crypt_filter(self) -> bool:
        return '/Crypt' in (name for name, _ in self._filters())

    def add_crypt_filter(
        self,
        name=NameObject('/Identity'),
        params=None,
        handler: Optional['SecurityHandler'] = None,
    ):
        if handler is not None:
            self._handler = handler

        if self._handler is None:
            raise PdfStreamError("There is no security handler around")

        if name not in self._handler.crypt_filter_config:
            raise PdfStreamError(
                f"The crypt filter {name} is not known to the security handler."
            )
        params = params or DictionaryObject()
        params['/Type'] = pdf_name('/CryptFilterDecodeParms')
        params['/Name'] = name
        self.apply_filter(
            pdf_name('/Crypt'), params=params, allow_duplicates=True
        )

    def _filters(self) -> Iterator[Tuple[str, Optional[dict]]]:
        try:
            filter_arr = self[pdf_name('/Filter')]
        except KeyError:
            return

        if isinstance(filter_arr, NameObject):
            # we have a single filter instance
            filter_arr = (filter_arr,)
        elif not isinstance(filter_arr, ArrayObject):
            raise PdfStreamError(
                '/Filter should be a name object or an array of names.'
            )

        try:
            decode_params = self[pdf_name('/DecodeParms')]
            if isinstance(decode_params, DictionaryObject):
                # one instance
                decode_params = [decode_params]
            if isinstance(decode_params, list):
                lendiff = len(filter_arr) - len(decode_params)
                # this should be zero, but let's be lenient
                if lendiff > 0:
                    decode_params += [NullObject()] * lendiff
        except KeyError:
            decode_params = [NullObject()] * len(filter_arr)

        # make sure to deal with resolving decrypted object proxies by
        # calling get_object()
        yield from zip(
            filter_arr, (param_set.get_object() for param_set in decode_params)
        )

    def _stream_decoders(self):
        from . import filters

        for filter_type, params in self._filters():
            try:
                if params is None or isinstance(params, NullObject):
                    params = {}
                if filter_type == '/Crypt':
                    # crypt filters get special treatment
                    # if we're dealing with the identity filter, just move on
                    if params.get('/Name', '/Identity') == '/Identity':
                        continue
                    # if it's another one, we need a reference to the security
                    # handler
                    sh = self._handler
                    if sh is None:
                        raise PdfStreamError(
                            "PDF streams require a security handler to use "
                            "explicit /Crypt filters."
                        )
                    decoder = filters.CryptFilterDecoder(sh)
                else:
                    decoder = filters.get_generic_decoder(filter_type)
                yield decoder, params
            except KeyError:
                raise NotImplementedError(
                    "Filters of type %s are not supported." % filter_type
                )

    def strip_filters(self):
        """
        Ensure the stream is decoded, and remove any filters.
        """

        self._data = self._encoded_data = self.data
        self.pop(pdf_name('/Filter'), None)
        self.pop(pdf_name('/DecodeParms'), None)

    @property
    def data(self) -> bytes:
        """
        Return the decoded stream data as bytes.
        If the stream hasn't been decoded yet, it will be decoded on-the-fly.

        :raises .misc.PdfStreamError:
            If the stream could not be decoded.
        """
        if self._data is None:
            data = self._encoded_data
            if data is None:
                raise PdfStreamError("No data available.")
            for filter_cls, decode_params in self._stream_decoders():
                data = filter_cls.decode(data, decode_params)
            if isinstance(data, memoryview):
                data = data.tobytes()
            self._data = data
        assert self._data is not None
        return self._data

    @property
    def encoded_data(self) -> bytes:
        """
        Return the encoded stream data as bytes.
        If the stream hasn't been encoded yet, it will be encoded on-the-fly.

        :raises .misc.PdfStreamError:
            If the stream could not be encoded.
        """
        if self._encoded_data is None:
            data = self._data
            if data is None:
                raise PdfStreamError("No data available.")
            decoders = tuple(self._stream_decoders())
            for filter_cls, decode_params in reversed(decoders):
                data = filter_cls.encode(data, decode_params)
            self._encoded_data = data
        assert self._encoded_data is not None
        return self._encoded_data

    def apply_filter(
        self, filter_name, params=None, allow_duplicates: Optional[bool] = True
    ):
        """
        Apply a new filter to this stream. This filter will be prepended
        to any existing filters.
        This means that is is placed *last* in the encoding order, but *first*
        in the decoding order.

        *Note:* Calling this method on an encoded stream will first cause the
        stream to be decoded using the filters already present.
        The cached value for the encoded stream data will be cleared.

        :param filter_name:
            Name of the filter
            (see :const:`~pyhanko.pdf_utils.filters.DECODERS`)
        :param params:
            Parameters to the filter (will be written to ``/DecodeParms`` if
            not ``None``)
        :param allow_duplicates:
            If ``None``, silently ignore duplicate filters.
            If ``False``, raise ValueError when attempting to add a duplicate
            filter. If ``True`` (default), duplicate filters are allowed.
        """
        # If the stream already contains (encoded) data, we have to reencode it
        # later on, which requires a decoding operation.
        data = self._data
        if data is None and self._encoded_data is not None:
            data = self.data

        # ... and list all current filters with their parameters.
        cur_filters = list(self._filters())
        # normalise the input parameters
        if not isinstance(filter_name, NameObject):
            filter_name = pdf_name(filter_name)
        if params is not None and not isinstance(params, DictionaryObject):
            params = DictionaryObject(params)
        if not cur_filters:
            # only one filter, so don't write arrays
            self[pdf_name('/Filter')] = filter_name
            if params:
                self[pdf_name('/DecodeParms')] = params
        else:
            # FIXME deal with shortened names for standard filters
            # split cur_filters back into two pieces
            filter_names, param_sets = zip(*cur_filters)
            if not allow_duplicates and filter_name in filter_names:
                if allow_duplicates is False:
                    raise PdfWriteError(
                        f'Filter {filter_name} has already been applied to '
                        f'this stream.'
                    )
                else:
                    # Silently ignore
                    return

            # prepend the new filter (order is important!)
            self[pdf_name('/Filter')] = ArrayObject(
                (filter_name,) + filter_names
            )

            if params or any(param_sets):

                def _params():
                    yield params or NullObject()
                    for param_set in param_sets:
                        yield param_set or NullObject()

                self[pdf_name('/DecodeParms')] = ArrayObject(_params())
        self._encoded_data = None
        self._data = data

    def compress(self):
        """
        Convenience method to add a ``/FlateDecode`` filter with default
        settings, if one is not already present.

        *Note:* compression is not actually applied until the stream is written.
        """
        self.apply_filter(pdf_name('/FlateDecode'), allow_duplicates=None)

    @property
    def is_embedded_file_stream(self):
        try:
            return self.raw_get('/Type') == '/EmbeddedFile'
        except KeyError:
            return False

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        data = self.encoded_data
        if (
            handler is not None
            and container_ref is not None
            and not self._has_crypt_filter
        ):
            cf = handler.get_stream_filter()
            local_key = cf.derive_object_key(
                container_ref.idnum, container_ref.generation
            )
            data = cf.encrypt(local_key, data)
        self[NameObject("/Length")] = NumberObject(len(data))
        # write the dictionary
        super().write_to_stream(stream, handler, container_ref)
        del self["/Length"]
        stream.write(b"\nstream\n")
        stream.write(data)
        stream.write(b"\nendstream")


def encode_pdfdocencoding(unicode_string):
    def _build():
        for c in unicode_string:
            try:
                yield _pdfDocEncoding_rev[c]
            except KeyError:
                raise UnicodeEncodeError(
                    "pdfdocencoding",
                    c,
                    -1,
                    -1,
                    "does not exist in translation table",
                )

    return bytes(_build())


def decode_pdfdocencoding(byte_array):
    def _build():
        for b in byte_array:
            c = _pdfDocEncoding[b]
            if c == '\u0000':
                raise UnicodeDecodeError(
                    "pdfdocencoding",
                    bytes((b,)),
                    -1,
                    -1,
                    "does not exist in translation table",
                )
            yield c

    return ''.join(_build())


_pdfDocEncoding = (
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u0000',
    '\u02d8',
    '\u02c7',
    '\u02c6',
    '\u02d9',
    '\u02dd',
    '\u02db',
    '\u02da',
    '\u02dc',
    '\u0020',
    '\u0021',
    '\u0022',
    '\u0023',
    '\u0024',
    '\u0025',
    '\u0026',
    '\u0027',
    '\u0028',
    '\u0029',
    '\u002a',
    '\u002b',
    '\u002c',
    '\u002d',
    '\u002e',
    '\u002f',
    '\u0030',
    '\u0031',
    '\u0032',
    '\u0033',
    '\u0034',
    '\u0035',
    '\u0036',
    '\u0037',
    '\u0038',
    '\u0039',
    '\u003a',
    '\u003b',
    '\u003c',
    '\u003d',
    '\u003e',
    '\u003f',
    '\u0040',
    '\u0041',
    '\u0042',
    '\u0043',
    '\u0044',
    '\u0045',
    '\u0046',
    '\u0047',
    '\u0048',
    '\u0049',
    '\u004a',
    '\u004b',
    '\u004c',
    '\u004d',
    '\u004e',
    '\u004f',
    '\u0050',
    '\u0051',
    '\u0052',
    '\u0053',
    '\u0054',
    '\u0055',
    '\u0056',
    '\u0057',
    '\u0058',
    '\u0059',
    '\u005a',
    '\u005b',
    '\u005c',
    '\u005d',
    '\u005e',
    '\u005f',
    '\u0060',
    '\u0061',
    '\u0062',
    '\u0063',
    '\u0064',
    '\u0065',
    '\u0066',
    '\u0067',
    '\u0068',
    '\u0069',
    '\u006a',
    '\u006b',
    '\u006c',
    '\u006d',
    '\u006e',
    '\u006f',
    '\u0070',
    '\u0071',
    '\u0072',
    '\u0073',
    '\u0074',
    '\u0075',
    '\u0076',
    '\u0077',
    '\u0078',
    '\u0079',
    '\u007a',
    '\u007b',
    '\u007c',
    '\u007d',
    '\u007e',
    '\u0000',
    '\u2022',
    '\u2020',
    '\u2021',
    '\u2026',
    '\u2014',
    '\u2013',
    '\u0192',
    '\u2044',
    '\u2039',
    '\u203a',
    '\u2212',
    '\u2030',
    '\u201e',
    '\u201c',
    '\u201d',
    '\u2018',
    '\u2019',
    '\u201a',
    '\u2122',
    '\ufb01',
    '\ufb02',
    '\u0141',
    '\u0152',
    '\u0160',
    '\u0178',
    '\u017d',
    '\u0131',
    '\u0142',
    '\u0153',
    '\u0161',
    '\u017e',
    '\u0000',
    '\u20ac',
    '\u00a1',
    '\u00a2',
    '\u00a3',
    '\u00a4',
    '\u00a5',
    '\u00a6',
    '\u00a7',
    '\u00a8',
    '\u00a9',
    '\u00aa',
    '\u00ab',
    '\u00ac',
    '\u0000',
    '\u00ae',
    '\u00af',
    '\u00b0',
    '\u00b1',
    '\u00b2',
    '\u00b3',
    '\u00b4',
    '\u00b5',
    '\u00b6',
    '\u00b7',
    '\u00b8',
    '\u00b9',
    '\u00ba',
    '\u00bb',
    '\u00bc',
    '\u00bd',
    '\u00be',
    '\u00bf',
    '\u00c0',
    '\u00c1',
    '\u00c2',
    '\u00c3',
    '\u00c4',
    '\u00c5',
    '\u00c6',
    '\u00c7',
    '\u00c8',
    '\u00c9',
    '\u00ca',
    '\u00cb',
    '\u00cc',
    '\u00cd',
    '\u00ce',
    '\u00cf',
    '\u00d0',
    '\u00d1',
    '\u00d2',
    '\u00d3',
    '\u00d4',
    '\u00d5',
    '\u00d6',
    '\u00d7',
    '\u00d8',
    '\u00d9',
    '\u00da',
    '\u00db',
    '\u00dc',
    '\u00dd',
    '\u00de',
    '\u00df',
    '\u00e0',
    '\u00e1',
    '\u00e2',
    '\u00e3',
    '\u00e4',
    '\u00e5',
    '\u00e6',
    '\u00e7',
    '\u00e8',
    '\u00e9',
    '\u00ea',
    '\u00eb',
    '\u00ec',
    '\u00ed',
    '\u00ee',
    '\u00ef',
    '\u00f0',
    '\u00f1',
    '\u00f2',
    '\u00f3',
    '\u00f4',
    '\u00f5',
    '\u00f6',
    '\u00f7',
    '\u00f8',
    '\u00f9',
    '\u00fa',
    '\u00fb',
    '\u00fc',
    '\u00fd',
    '\u00fe',
    '\u00ff',
)

assert len(_pdfDocEncoding) == 256

_pdfDocEncoding_rev = {char: ix for ix, char in enumerate(_pdfDocEncoding)}

pdf_name = NameObject
PROXYABLE = (TextStringObject, ByteStringObject, DictionaryObject, ArrayObject)


def proxy_encrypted_obj(encrypted_obj, handler):
    if isinstance(encrypted_obj, PROXYABLE):
        return DecryptedObjectProxy(encrypted_obj, handler)
    else:
        return encrypted_obj


class DecryptedObjectProxy(PdfObject):
    """
    Internal proxy class that allows transparent on-demand encryption
    of objects.

    .. warning::
        Most public-facing APIs won't leave you to deal with these *directly*
        (that's half the reason this class exists in the first place), and
        the API of this class is considered internal.

        However, for reasons related to the historical PyPDF2 codebase from
        which pyHanko's object handling code ultimately derives, there are
        some Python builtins that might cause these wrapper objects to
        inadvertently "leak". Please `tell us about such cases
        <https://github.com/MatthiasValvekens/pyHanko/discussions>`_ so we can
        make those types of access more convenient and robust.

    .. danger::
        The ``__eq__`` implementation on this class is not safe for general use,
        due to the fact that certain structures in PDF are exempt from
        encryption. Only compare proxy objects with ``==`` in areas of the
        document where these exemptions don't apply.

    :param raw_object:
        A raw object, typically as-parsed from a PDF file.
    :param handler:
        The security handler governing this object.
    """

    raw_object: PdfObject
    """
    The underlying raw object, in its encrypted state.
    """

    def __init__(self, raw_object: PdfObject, handler):
        self.raw_object = raw_object
        self._decrypted: Optional[PdfObject] = None
        self.handler = handler

    @property
    def decrypted(self) -> PdfObject:
        """
        The decrypted PDF object exposed as a property.

        If this object is a container object, its constituent parts will be
        wrapped in :class:`.DecryptedObjectProxy` as well, in order to defer
        further decryption until the values are requested through a getter
        method on the container.
        """

        if self._decrypted is not None:
            return self._decrypted

        from .crypt import SecurityHandler

        decrypted: PdfObject

        obj = self.raw_object
        handler: SecurityHandler = self.handler
        container_ref = obj.container_ref
        if not isinstance(container_ref, Reference):
            raise ValueError(
                "Proxyable objects must have a container ref pointing to a "
                f"numbered object, not '{container_ref}'."
            )  # pragma: nocover
        if isinstance(obj, ByteStringObject) or isinstance(
            obj, TextStringObject
        ):
            cf = handler.get_string_filter()
            local_key = cf.derive_object_key(
                container_ref.idnum, container_ref.generation
            )
            decrypted = pdf_string(cf.decrypt(local_key, obj.original_bytes))
        elif isinstance(obj, DictionaryObject):
            decrypted_entries = {
                dictkey: proxy_encrypted_obj(value, handler)
                for dictkey, value in obj.items()
            }
            if isinstance(obj, StreamObject):
                decrypted = obj._implicit_decrypt_stream_content(
                    handler, container_ref, decrypted_entries
                )
            else:
                decrypted = DictionaryObject(decrypted_entries)
        elif isinstance(obj, ArrayObject):
            decrypted_map = map(lambda v: proxy_encrypted_obj(v, handler), obj)
            decrypted = ArrayObject(decrypted_map)
        else:  # pragma: nocover
            raise TypeError(f'Object of type {type(obj)} is not proxyable.')
        decrypted.container_ref = obj.container_ref
        self._decrypted = decrypted
        return decrypted

    def write_to_stream(
        self,
        stream,
        handler: Optional['SecurityHandler'] = None,
        container_ref=None,
    ):
        # maybe the encryption key for this object changed (due to it being
        # included as part of a larger object or somesuch, without proper
        # dereferencing), so to avoid unexpected shenanigans, let's start from
        # scratch.
        self.decrypted.write_to_stream(stream, handler, container_ref)

    def get_object(self):
        return self.decrypted.get_object()

    @property
    def container_ref(self):
        return self.raw_object.container_ref

    def __eq__(self, other):
        # NOTE: this will fail if the dictionary contains "un-decryptable"
        #  descendants! The diff_analysis module is aware of this restriction,
        #  but you probably shouldn't use this __eq__ method to compare
        #  arbitrary objects in a PDF file.
        return (
            isinstance(other, DecryptedObjectProxy)
            and other.decrypted == self.decrypted
        )


ASN_DT_FORMAT = "D:%Y%m%d%H%M%S"


def pdf_date(dt: datetime) -> TextStringObject:
    """
    Convert a datetime object into a PDF string.
    This function supports both timezone-aware and naive datetime objects.

    :param dt:
        The datetime object to convert.
    :return:
        A :class:`TextStringObject` representing the datetime passed in.
    """

    base_dt = dt.strftime(ASN_DT_FORMAT)
    utc_offset_string = ''
    utc_offset = dt.utcoffset()
    if utc_offset is not None:
        # compute UTC offset string
        tz_seconds = utc_offset.total_seconds()
        if not tz_seconds:
            utc_offset_string = 'Z'
        else:
            sign = '+'
            if tz_seconds < 0:
                sign = '-'
                tz_seconds = abs(tz_seconds)
            hrs, tz_seconds = divmod(tz_seconds, 3600)
            mins = tz_seconds // 60
            # XXX the apostrophe after the minute part of the offset is NOT
            #  what's in the spec, but Adobe Reader DC refuses to validate
            #  signatures with a date string that doesn't contain it.
            #  No idea why.
            utc_offset_string = sign + ("%02d'%02d'" % (hrs, mins))

    return TextStringObject(base_dt + utc_offset_string)


# The year field is the only mandatory one
MIN_DATE_REGEX = re.compile(r'^D:(\d{4})')
MIN_DATE_REGEX_LENIENT = re.compile(r'^(?:D:)?(\d{4})')
TWO_DIGIT_START = re.compile(r'^(\d\d)')
UTC_OFFSET = re.compile(r"(\d\d)(?:'(\d\d))?'?")


def parse_pdf_date(date_str: str, strict: bool = True) -> datetime:
    m = (MIN_DATE_REGEX if strict else MIN_DATE_REGEX_LENIENT).match(date_str)
    if not m:
        raise PdfReadError(f"{date_str} does not appear to be a date string.")
    year = int(m.group(1))

    # now, there are a number of 2-digit groups (anywhere from 0 to 5)
    date_remaining = date_str[m.end(0) :]
    lower_order = [1, 1, 0, 0, 0]

    for ix in range(5):
        m = TWO_DIGIT_START.match(date_remaining)
        if not m:
            break
        lower_order[ix] = int(m.group(1))
        date_remaining = date_remaining[2:]

    # TODO range checks
    month, day, hour, minute, second = lower_order

    # finally, parse the timezone
    tz_info = None
    if date_remaining:
        sgn = date_remaining[0]
        if sgn == 'Z' and len(date_remaining) == 1:
            tz_offset = timedelta(0)
        elif sgn in ('+', '-'):
            tz_spec = date_remaining[1:]
            tz_match = UTC_OFFSET.fullmatch(tz_spec)
            if not tz_match:
                raise PdfReadError(
                    f"Improper timezone specification in {date_str}: {tz_spec}"
                )
            tz_hours = int(tz_match.group(1))
            tz_minutes = int(tz_match.group(2) or 0)
            tz_offset = timedelta(hours=tz_hours, minutes=tz_minutes)
            if sgn == '-':
                tz_offset = -tz_offset
        else:
            raise PdfReadError(f"Improper trailing characters in {date_str}.")
        tz_info = timezone(tz_offset)

    try:
        return datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=second,
            microsecond=0,
            tzinfo=tz_info,
        )
    except ValueError as e:
        raise PdfReadError("Improper date value", e)
