"""
Utility to read PDF files.
Contains code from the PyPDF2 project; see :ref:`here <pypdf2-license>`
for the original license.

The implementation was tweaked with the express purpose of facilitating
historical inspection and auditing of PDF files with multiple revisions
through incremental updates.
This comes at a cost, and future iterations of this module may offer more
flexibility in terms of the level of detail with which file size is scrutinised.
"""

import logging
import os
import re
from collections import defaultdict
from io import BytesIO
from typing import BinaryIO, Dict, Generator, Optional, Set, Tuple, Union

from . import generic, misc
from .crypt import (
    AuthResult,
    AuthStatus,
    EnvelopeKeyDecrypter,
    PubKeySecurityHandler,
    SecurityHandler,
    StandardSecurityHandler,
)
from .crypt.api import PdfMacStatus
from .metadata.info import view_from_info_dict
from .metadata.model import DocumentMetadata
from .misc import PdfReadError, PdfStrictReadError
from .rw_common import PdfHandler
from .xref import (
    ObjStreamRef,
    TrailerDictionary,
    XRefBuilder,
    XRefCache,
    read_object_header,
)

logger = logging.getLogger(__name__)

__all__ = [
    'PdfFileReader',
    'HistoricalResolver',
    'parse_catalog_version',
    'RawPdfPath',
    'process_data_at_eof',
]

header_regex = re.compile(b'%PDF-(\\d).(\\d)')
catalog_version_regex = re.compile(r'/(\d).(\d)')


def parse_catalog_version(version_str) -> Optional[Tuple[int, int]]:
    m = catalog_version_regex.match(str(version_str))
    if m is not None:
        major = int(m.group(1))
        minor = int(m.group(2))
        return major, minor
    return None


# General remark:
# PyPDF2 parses all files backwards.
# This means that "next" and "previous" usually mean the opposite of what one
#  might expect.


def read_next_end_line(stream):
    def _build():
        while True:
            # Prevent infinite loops in malformed PDFs
            if stream.tell() == 0:
                raise PdfReadError("Could not read malformed PDF file")
            x = stream.read(1)
            if stream.tell() < 2 or not x:
                raise PdfReadError("EOL marker not found")
            stream.seek(-2, os.SEEK_CUR)
            if x == b'\n' or x == b'\r':
                break
            yield ord(x)
        crlf = False
        while x == b'\n' or x == b'\r':
            x = stream.read(1)
            if x == b'\n' or x == b'\r':  # account for CR+LF
                stream.seek(-1, os.SEEK_CUR)
                crlf = True
            stream.seek(-2, os.SEEK_CUR)
        # if using CR+LF, go back 2 bytes, else 1
        stream.seek(2 if crlf else 1, os.SEEK_CUR)

    return bytes(reversed(tuple(_build())))


def process_data_at_eof(stream) -> int:
    """
    Auxiliary function that reads backwards from the current position
    in a stream to find the EOF marker and startxref value

    This is internal API.

    :param stream:
        A stream to read from
    :return:
        The value of the startxref pointer, if found.
        Otherwise a PdfReadError is raised.
    """

    # offset of last 1024 bytes of stream
    last_1k = stream.tell() - 1024 + 1
    line = b''
    while line[:5] != b"%%EOF":
        if stream.tell() < last_1k:
            raise PdfReadError("EOF marker not found")
        line = read_next_end_line(stream)

    # find startxref entry - the location of the xref table
    line = read_next_end_line(stream)
    try:
        startxref = int(line)
    except ValueError:
        # 'startxref' may be on the same line as the location
        if not line.startswith(b"startxref"):
            raise PdfReadError("startxref not found")
        startxref = int(line[9:].strip())
        logger.warning("startxref on same line as offset")
    else:
        line = read_next_end_line(stream)
        if line[:9] != b"startxref":
            raise PdfReadError("startxref not found")

    return startxref


def _read_header_version(stream: BinaryIO) -> Tuple[int, int]:
    stream.seek(0)
    input_version = None
    header = misc.read_until_whitespace(stream, maxchars=20)
    # match ignores trailing chars
    m = header_regex.match(header)
    if m is not None:
        major = int(m.group(1))
        minor = int(m.group(2))
        input_version = (major, minor)
    if input_version is None:
        raise PdfReadError('Illegal PDF header')
    return input_version


def _read_xrefs_and_trailer(
    stream: BinaryIO, handler_ref: PdfHandler, strict: bool
) -> Tuple[XRefCache, XRefBuilder]:
    # start at the end to read the trailer & xref table
    stream.seek(-1, os.SEEK_END)
    # This needs to be recorded for incremental update purposes
    last_startxref = process_data_at_eof(stream)

    # Read the xref table
    xref_builder = XRefBuilder(
        handler=handler_ref,
        stream=stream,
        strict=strict,
        last_startxref=last_startxref,
    )
    xref_sections = xref_builder.read_xrefs()
    xref_cache = XRefCache(handler_ref, xref_sections)
    return xref_cache, xref_builder


class PdfFileReader(PdfHandler):
    """Class implementing functionality to read a PDF file and cache
    certain data about it."""

    last_startxref = None
    has_xref_stream = False
    xrefs: XRefCache

    def __init__(self, stream, strict: bool = True):
        """
        Initializes a PdfFileReader object.  This operation can take some time,
        as the PDF stream's cross-reference tables are read into memory.

        :param stream: A File object or an object that supports the standard
            read and seek methods similar to a File object.
        :param bool strict: Determines whether user should be warned of all
            problems and also causes some correctable problems to be fatal.
            Defaults to ``True``.
        """
        self._security_handler: Optional[SecurityHandler] = None
        self.strict = strict
        self.resolved_objects: Dict[Tuple[int, int], generic.PdfObject] = {}
        self._header_version = None
        self._input_version = None
        self._historical_resolver_cache: Dict[int, HistoricalResolver] = {}
        self.stream = stream
        # first, read the header & PDF version number
        # (version number can be overridden in the document catalog later)
        self._header_version = _read_header_version(stream)
        self.xrefs, xref_builder = _read_xrefs_and_trailer(stream, self, strict)
        self.last_startxref = xref_builder.last_startxref
        self.trailer = xref_builder.trailer
        self.has_xref_stream = xref_builder.has_xref_stream

        self._embedded_signatures = None

    @property
    def security_handler(self):
        if self.encrypt_dict and not self._security_handler:
            self._security_handler = SecurityHandler.build(self.encrypt_dict)
        return self._security_handler

    def _xmp_meta_view(self) -> Optional[DocumentMetadata]:
        try:
            from pyhanko.pdf_utils.metadata import xmp_xml

            meta_obj = self.root['/Metadata']
        except (ImportError, KeyError):
            return None

        if isinstance(meta_obj, xmp_xml.MetadataStream):
            return xmp_xml.meta_from_xmp(meta_obj.xmp)
        return None

    @property
    def document_meta_view(self) -> DocumentMetadata:
        xmp_view = self._xmp_meta_view()
        if xmp_view is not None:
            return xmp_view

        try:
            info_dict = self.trailer_view['/Info']
        except KeyError:
            return DocumentMetadata()

        return view_from_info_dict(info_dict, strict=self.strict)

    @property
    def input_version(self):
        input_version = self._input_version
        if input_version is not None:
            return input_version
        header_version = self._header_version

        try:
            version = self.root['/Version']
            input_version = parse_catalog_version(version) or header_version
        except KeyError:
            input_version = header_version

        self._input_version = input_version
        return input_version

    def _get_object_from_stream(self, idnum, stmnum, idx):
        # indirect reference to object in object stream
        # read the entire object stream into memory
        stream_ref = generic.Reference(stmnum, 0, self)
        stream = stream_ref.get_object()
        assert isinstance(stream, generic.StreamObject)
        # This is an xref to a stream, so its type better be a stream
        assert stream['/Type'] == '/ObjStm'
        # /N is the number of indirect objects in the stream
        if not (0 <= idx < stream['/N']):
            if self.strict:
                raise PdfStrictReadError("Object stream does not contain index")
            else:
                return generic.NullObject()
        stream_data = BytesIO(stream.data)
        first_object = stream['/First']
        for i in range(stream['/N']):
            try:
                misc.read_non_whitespace(stream_data, seek_back=True)
                objnum = generic.NumberObject.read_from_stream(stream_data)
                misc.read_non_whitespace(stream_data, seek_back=True)
                offset = generic.NumberObject.read_from_stream(stream_data)
                misc.read_non_whitespace(stream_data, seek_back=True)
            except ValueError:
                if self.strict:
                    raise PdfStrictReadError(
                        "Object stream header possibly corrupted"
                    )
                else:
                    return generic.NullObject()
            if objnum != idnum:
                # We're only interested in one object
                continue
            if self.strict and idx != i:
                raise PdfStrictReadError("Object is in wrong index.")
            obj_start = first_object + offset
            stream_data.seek(obj_start)
            try:
                obj = generic.read_object(
                    stream_data,
                    generic.Reference(idnum, 0, self),
                )
            except (misc.PdfStreamError, misc.PdfReadError) as e:
                # Stream object cannot be read. Normally, a critical error, but
                # Adobe Reader doesn't complain, so continue (in strict mode?)
                logger.warning(
                    f"Invalid stream (index {i}) within object {idnum} 0: {e}"
                )

                if self.strict:
                    raise PdfStrictReadError("Can't read object stream: %s" % e)
                # Replace with null. Hopefully it's nothing important.
                obj = generic.NullObject()
            if isinstance(obj, (generic.StreamObject, generic.IndirectObject)):
                if self.strict:
                    raise PdfStrictReadError(
                        "Encountered forbidden object type in object stream"
                    )
                else:
                    return obj

            generic.read_non_whitespace(
                stream_data, seek_back=True, allow_eof=True
            )
            return obj

        if self.strict:
            raise PdfStrictReadError(
                "Object not found in stream, "
                "this is a fatal error in strict mode"
            )
        else:
            return generic.NullObject()

    @property
    def encrypt_dict(self) -> Optional[generic.DictionaryObject]:
        try:
            encrypt_ref = self.trailer.raw_get('/Encrypt')
        except KeyError:
            return None
        if isinstance(encrypt_ref, generic.IndirectObject):
            encrypt_dict = self.get_object(
                encrypt_ref.reference, never_decrypt=True
            )
        elif not self.strict:
            encrypt_dict = encrypt_ref
        else:
            raise misc.PdfReadError(
                "Encryption settings must be an indirect reference"
            )
        if not isinstance(encrypt_dict, generic.DictionaryObject):
            raise misc.PdfReadError("Encryption settings must be a dictionary")
        return encrypt_dict

    @property
    def trailer_view(self) -> generic.DictionaryObject:
        return self.trailer.flatten()

    @property
    def root_ref(self) -> generic.Reference:
        return self.trailer.raw_get(
            '/Root', decrypt=generic.EncryptedObjAccess.RAW
        ).reference

    @property
    def document_id(self) -> Tuple[bytes, bytes]:
        id_arr = self.trailer['/ID']
        return id_arr[0].original_bytes, id_arr[1].original_bytes

    def get_historical_root(self, revision: int):
        """
        Get the document catalog for a specific revision.

        :param revision:
            The revision to query, the oldest one being `0`.
        :return:
            The value of the document catalog dictionary for that revision.
        """
        return self.get_historical_resolver(revision).root

    @property
    def total_revisions(self) -> int:
        """
        :return:
            The total number of revisions made to this file.
        """
        return self.xrefs.total_revisions

    def get_object(
        self,
        ref,
        revision=None,
        never_decrypt=False,
        transparent_decrypt=True,
        as_metadata_stream=False,
    ):
        """
        Read an object from the input stream.

        :param ref:
            :class:`~.generic.Reference` to the object.
        :param revision:
            Revision number, to return the historical value of a reference.
            This always bypasses the cache.
            The oldest revision is numbered `0`.
            See also :class:`.HistoricalResolver`.
        :param never_decrypt:
            Skip decryption step (only needed for parsing ``/Encrypt``)
        :param transparent_decrypt:
            If ``True``, all encrypted objects are transparently decrypted by
            default (in the sense that a user of the API in a PyPDF2 compatible
            way would only "see" decrypted objects).
            If ``False``, this method may return a proxy object that still
            allows access to the "original".

            .. danger::
                The encryption parameters are considered internal,
                undocumented API, and subject to change without notice.
        :param as_metadata_stream:
            Whether to dereference the object as an XMP metadata stream.
        :return:
            A :class:`~.generic.PdfObject`.
        :raises PdfReadError:
            Raised if there is an issue reading the object from the file.
        """
        # Ensure that when an Xref stream is queried by ID, we
        # don't try to decrypt it.
        if ref in self.xrefs.xref_stream_refs:
            never_decrypt = True
        if revision is None:
            obj = self.cache_get_indirect_object(ref.generation, ref.idnum)
            if obj is None:
                obj = self._read_object(
                    ref,
                    self.xrefs[ref],
                    never_decrypt=never_decrypt,
                    as_metadata_stream=as_metadata_stream,
                )
                # cache before (potential) decrypting
                self.cache_indirect_object(ref.generation, ref.idnum, obj)
        else:
            # never cache historical refs
            marker = self.xrefs.get_historical_ref(ref, revision)
            if marker is None:
                logger.warning(
                    f'Could not find object ({ref.idnum} {ref.generation}) '
                    f'in history at revision {revision}.'
                )
            obj = self._read_object(
                ref,
                marker,
                never_decrypt=never_decrypt,
                as_metadata_stream=as_metadata_stream,
            )

        if transparent_decrypt and isinstance(
            obj, generic.DecryptedObjectProxy
        ):
            obj = obj.decrypted

        return obj

    def _read_object(
        self,
        ref: generic.Reference,
        marker: Union[int, ObjStreamRef, None],
        never_decrypt: bool = False,
        as_metadata_stream: bool = False,
    ):
        if marker is None:
            if self.strict:
                raise PdfStrictReadError(
                    f"Object addressed by {ref} not found in the current "
                    f"context. This is an error in strict mode."
                )
            else:
                logger.info(
                    f"Object addressed by {ref} not found in the current "
                    f"context, substituting null in non-strict mode."
                )
                obj = generic.NullObject()
                obj.container_ref = ref
                return obj
        elif isinstance(marker, ObjStreamRef):
            # object in object stream
            retval = self._get_object_from_stream(
                ref.idnum, marker.obj_stream_id, marker.ix_in_stream
            )
        else:
            obj_start = marker
            # standard indirect object
            self.stream.seek(obj_start)
            idnum, generation = read_object_header(
                self.stream, strict=self.strict
            )
            if idnum != ref.idnum or generation != ref.generation:
                raise PdfReadError(
                    f"Expected object ID ({ref.idnum} {ref.generation}) "
                    f"does not match actual ({idnum} {generation})."
                )
            retval = generic.read_object(
                self.stream,
                generic.Reference(idnum, generation, self),
                as_metadata_stream=as_metadata_stream,
            )
            generic.read_non_whitespace(self.stream, seek_back=True)
            obj_data_end = self.stream.tell() - 1
            endobj = self.stream.read(6)
            if endobj != b'endobj':
                if self.strict:  # pragma: nocover
                    raise PdfStrictReadError(
                        f'Expected endobj marker at position {obj_data_end} '
                        f'but found {repr(endobj)}'
                    )
            else:
                generic.read_non_whitespace(self.stream, seek_back=True)

        # override encryption is used for the /Encrypt dictionary
        # and objects inside object streams should also remain unencrypted
        if (
            not never_decrypt
            and not isinstance(marker, ObjStreamRef)
            and self.encrypted
        ):
            sh: Optional[SecurityHandler] = self.security_handler
            assert sh is not None
            # make sure the object that lands in the cache is always
            # a proxy object
            retval = generic.proxy_encrypted_obj(retval, sh)
        return retval

    def cache_get_indirect_object(self, generation, idnum):
        out = self.resolved_objects.get((generation, idnum))
        return out

    def cache_indirect_object(self, generation, idnum, obj):
        self.resolved_objects[(generation, idnum)] = obj
        return obj

    def _validate_pdf_mac(self, prelim_auth_result: AuthResult):
        sh = self.security_handler
        assert sh is not None

        # TODO add MAC enforcement policy param
        # TODO forbid empty user pws (in the standard SH)
        # Default strategy:
        # we ALWAYS enforce the MAC if the security handler says that support
        # is enabled (checked by presence of KDFSalt entry), or if there's
        # an auth code entry in the trailer.
        # In addition, we cross-check against permission bit 13 (1-indexed)
        # returned by the security handler.
        traces_of_mac = '/AuthCode' in self.trailer or sh.pdf_mac_enabled
        perms = prelim_auth_result.permission_flags
        mac_required = perms is not None and perms.mac_required()
        if prelim_auth_result.status != AuthStatus.FAILED and (
            traces_of_mac or mac_required
        ):
            from pyhanko.pdf_utils.crypt import pdfmac

            try:
                pdfmac.validate_pdf_mac(self)
                auth_result = AuthResult(
                    status=prelim_auth_result.status,
                    permission_flags=prelim_auth_result.permission_flags,
                    mac_status=PdfMacStatus.SUCCESSFUL,
                )
            except pdfmac.PdfMacValidationError as e:
                logger.warning(
                    f"Failed to validate MAC: {e.failure_message}", exc_info=e
                )
                auth_result = AuthResult(
                    status=AuthStatus.FAILED,
                    mac_status=PdfMacStatus.FAILED,
                    mac_failure_reason=e.failure_message,
                )
            return auth_result
        else:
            return prelim_auth_result

    def decrypt(self, password: Union[str, bytes]) -> AuthResult:
        """
        When using an encrypted PDF file with the standard PDF encryption
        handler, this function will allow the file to be decrypted.
        It checks the given password against the document's user password and
        owner password, and then stores the resulting decryption key if either
        password is correct.

        Both legacy encryption schemes and PDF 2.0 encryption (based on AES-256)
        are supported.

        .. danger::
            Supplying either user or owner password will work.
            Cryptographically, both allow the decryption key to be computed,
            but processors are expected to adhere to the ``/P`` flags in the
            encryption dictionary when accessing a file with the user password.
            Currently, pyHanko does not enforce these restrictions, but it
            may in the future.

        .. danger::
            One should also be aware that the legacy encryption schemes used
            prior to PDF 2.0 are (very) weak, and we only support them for
            compatibility reasons.
            Under no circumstances should these still be used to encrypt new
            files.

        :param password: The password to match.
        """
        sh = self.security_handler
        if not isinstance(sh, StandardSecurityHandler):
            raise misc.PdfReadError(
                f"Security handler is of type '{type(sh)}', "
                f"not StandardSecurityHandler"
            )  # pragma: nocover

        auth_result = sh.authenticate(password, id1=self.document_id[0])
        return self._validate_pdf_mac(auth_result)

    def decrypt_pubkey(self, credential: EnvelopeKeyDecrypter) -> AuthResult:
        """
        Decrypt a PDF file encrypted using public-key encryption by providing
        a credential representing the private key of one of the recipients.

        .. danger::
            The same caveats as in :meth:`.decrypt` w.r.t. permission handling
            apply to this method.

        .. danger::
            The robustness of the public key cipher being used is not the only
            factor in the security of public-key encryption in PDF.
            The standard still permits weak schemes to encrypt the actual file
            data and file keys.
            PyHanko uses sane defaults everywhere, but other software may not.

        :param credential:
            The :class:`.EnvelopeKeyDecrypter` handling the recipient's
            private key.
        """
        sh = self.security_handler
        if not isinstance(sh, PubKeySecurityHandler):
            raise misc.PdfReadError(
                f"Security handler is of type '{type(sh)}', "
                f"not PubKeySecurityHandler"
            )  # pragma: nocover
        auth_result = sh.authenticate(credential)
        return self._validate_pdf_mac(auth_result)

    @property
    def encrypted(self):
        """
        :return: ``True`` if a document is encrypted, ``False`` otherwise.
        """
        return self.security_handler is not None

    def get_historical_resolver(self, revision: int) -> 'HistoricalResolver':
        """
        Return a :class:`~.rw_common.PdfHandler` instance that provides a view
        on the file at a specific revision.

        :param revision:
            The revision number to use, with `0` being the oldest.
        :return:
            An instance of :class:`~.HistoricalResolver`.
        """
        cache = self._historical_resolver_cache
        try:
            return cache[revision]
        except KeyError:
            res = HistoricalResolver(self, revision)
            cache[revision] = res
            return res

    @property
    def embedded_signatures(self):
        """
        :return:
            The signature objects embedded in this document, in signing order;
            see :class:`~pyhanko.sign.validation.EmbeddedPdfSignature`.
        """
        if self._embedded_signatures is not None:
            return self._embedded_signatures
        from pyhanko.sign.fields import enumerate_sig_fields

        from ..sign.validation import EmbeddedPdfSignature

        sig_fields = enumerate_sig_fields(self, filled_status=True)

        result = sorted(
            (
                EmbeddedPdfSignature(self, sig_field, fq_name)
                for fq_name, sig_obj, sig_field in sig_fields
            ),
            key=lambda emb: emb.signed_revision,
        )
        self._embedded_signatures = result
        return result

    @property
    def embedded_regular_signatures(self):
        """
        :return:
            The signature objects of type ``/Sig`` embedded in this document,
            in signing order;
            see :class:`~pyhanko.sign.validation.EmbeddedPdfSignature`.
        """
        return [
            emb_sig
            for emb_sig in self.embedded_signatures
            if emb_sig.sig_object_type == '/Sig'
        ]

    @property
    def embedded_timestamp_signatures(self):
        """
        :return:
            The signature objects of type ``/DocTimeStamp`` embedded in
            this document, in signing order;
            see :class:`~pyhanko.sign.validation.EmbeddedPdfSignature`.
        """
        return [
            emb_sig
            for emb_sig in self.embedded_signatures
            if emb_sig.sig_object_type == '/DocTimeStamp'
        ]


class RawPdfPath:
    """
    Class to model raw paths in a file.

    This class is internal API.
    """

    def __init__(self, *path: Union[str, int]):
        self.path = path

    def __len__(self):
        return len(self.path)

    def __iter__(self):
        return iter(self.path)

    def _tag(self):
        # should give better hashing results
        return tuple(map(lambda x: (isinstance(x, int), x), self.path))

    def __hash__(self):
        return hash(self._tag())

    def __eq__(self, other):
        return isinstance(other, RawPdfPath) and (
            self is other or self._tag() == other._tag()
        )

    def walk_nodes(
        self, from_obj, transparent_dereference=True
    ) -> Generator[Tuple[Union[int, str, None], generic.PdfObject], None, None]:
        current_obj = from_obj
        elem: Union[int, str, None] = None
        for ix, entry in enumerate(self.path):
            if not transparent_dereference:
                yield elem, current_obj
            if isinstance(current_obj, generic.IndirectObject):
                current_obj = current_obj.get_object()
            if transparent_dereference:
                yield elem, current_obj
            if isinstance(entry, str):
                if isinstance(
                    current_obj, (generic.DictionaryObject, TrailerDictionary)
                ):
                    try:
                        current_obj = current_obj.raw_get(entry)
                        elem = entry
                        continue
                    except KeyError:
                        raise misc.PdfReadError(
                            f"Encountered missing dictionary "
                            f"entry {entry} at position {ix} in path {self} "
                            f"from {from_obj}."
                        )
            elif isinstance(entry, int):
                if isinstance(current_obj, generic.ArrayObject):
                    if not (0 <= entry <= len(current_obj)):
                        raise misc.PdfReadError(
                            f"Encountered out-of-range array index "
                            f"{entry} at position {ix} in path {self} "
                            f"from {from_obj}."
                        )
                    current_obj = current_obj.raw_get(entry)
                    elem = entry
                    continue
            # if we get here, there's a typing issue
            raise misc.PdfReadError(
                f"Type error in path {self} at position {ix}."
            )
        if (
            isinstance(current_obj, generic.IndirectObject)
            and transparent_dereference
        ):
            yield elem, current_obj.get_object()
        else:
            yield elem, current_obj

    def access_on(self, from_obj, dereference_last=True) -> generic.PdfObject:
        walk = self.walk_nodes(
            from_obj, transparent_dereference=dereference_last
        )
        obj = from_obj
        try:
            while True:
                _, obj = next(walk)
        except StopIteration:
            return obj

    def access_reference_on(self, from_obj) -> generic.Reference:
        ind_obj = self.access_on(from_obj, dereference_last=False)
        if not isinstance(ind_obj, generic.IndirectObject):
            raise misc.IndirectObjectExpected(
                f"Final entity on path {self} starting from {from_obj} is not "
                f"an indirect object."
            )
        return ind_obj.reference

    @staticmethod
    def _fmt_node(node):
        if isinstance(node, int):
            return '[%d]' % node
        else:
            return '.' + node[1:]

    def __add__(self, other):
        if isinstance(other, RawPdfPath):
            return RawPdfPath(*self.path, *other.path)
        elif isinstance(other, (int, str)):
            return RawPdfPath(*self.path, other)
        else:  # pragma: nocover
            raise TypeError

    def __str__(self):
        return ''.join(map(RawPdfPath._fmt_node, self.path))

    def __repr__(self):  # pragma: nocover
        return f"PathInRevision('{str(self)}')"


class HistoricalResolver(PdfHandler):
    """
    :class:`~.rw_common.PdfHandler` implementation that provides a view
    on a particular revision of a PDF file.

    Instances of :class:`.HistoricalResolver` should be created by calling the
    :meth:`~.PdfFileReader.get_historical_resolver` method on a
    :class:`~.PdfFileReader` object.

    Instances of this class cache the result of :meth:`get_object` calls.

    .. danger::
        This class is documented, but is nevertheless considered internal API,
        and easy to misuse.

        In particular, the `container_ref` attribute must *not* be relied upon
        for objects retrieved from a :class:`.HistoricalResolver`.
        Internally, it is only used to make lazy decryption work in historical
        revisions.

    .. note::
        Be aware that instances of this class transparently rewrite the PDF
        handler associated with any reference objects returned from the reader,
        so calling :meth:`~.generic.Reference.get_object` on an indirect
        reference object will cause the reference to be resolved within the
        selected revision.
    """

    @property
    def document_meta_view(self) -> DocumentMetadata:
        try:
            info_dict = self.trailer_view['/Info']
        except KeyError:
            return DocumentMetadata()

        return view_from_info_dict(info_dict, strict=self.reader.strict)

    @property
    def document_id(self) -> Tuple[bytes, bytes]:
        id_arr = self._trailer['/ID']
        return id_arr[0].original_bytes, id_arr[1].original_bytes

    def __init__(self, reader: PdfFileReader, revision):
        self.cache: Dict[generic.Reference, generic.PdfObject] = {}
        self.reader = reader
        self.revision = revision
        self._trailer = self.reader.trailer.flatten(self.revision)
        self._trailer.container_ref = generic.TrailerReference(self)
        self._indirect_object_access_cache: Optional[
            Dict[generic.Reference, Set[RawPdfPath]]
        ] = None

    @property
    def trailer_view(self) -> generic.DictionaryObject:
        return self._trailer

    def get_object(
        self, ref: generic.Reference, as_metadata_stream: bool = False
    ):
        cache = self.cache
        try:
            obj = cache[ref]
        except KeyError:
            # if the object wasn't modified after this revision
            # we can grab it from the "normal" shared cache.
            reader = self.reader
            revision = self.revision
            try:
                last_change = reader.xrefs.get_last_change(ref)
            except KeyError:
                logger.warning(
                    f"Could not determine history of {ref} in xref sections"
                )
                last_change = None
            if last_change is not None and last_change <= revision:
                obj = reader.get_object(
                    ref,
                    transparent_decrypt=False,
                    as_metadata_stream=as_metadata_stream,
                )
            else:
                obj = reader.get_object(
                    ref,
                    revision,
                    transparent_decrypt=False,
                    as_metadata_stream=as_metadata_stream,
                )

            # replace all PDF handler references in the object with references
            # to this one, so that indirect references will resolve within
            # this historical revision
            cache[ref] = obj = self._subsume_object(obj)
        if isinstance(obj, generic.DecryptedObjectProxy):
            return obj.decrypted
        return obj

    def _subsume_object(self, obj):
        # Dealing with encrypted objects is tricky:
        # - We can't lazily subsume
        # - We can (and must!) lazily decrypt: forcing decryption
        #   is effectively impossible since we don't know where we are in
        #   the file's object graph, which makes it impossible to avoid
        #   trying to decrypt special objects that are never to be encrypted
        #   in the first place.
        # The logical consequence of that is that we have to subsume the
        #  underlying raw object, and take care to set the container_ref
        #  on all proxyable object types as we pass through them.
        # If we don't do that, then the lazy decryption of our subsumed
        # DecryptedObjectProxies will fail downstream.

        # IMPORTANT NOTE: At the same time, we do NOT mess with container_ref
        # for non-proxiable types (i.e. primitives), since we do not clone
        # such objects when subsuming them, so we cannot safely override
        # the container_ref value.

        # first, recreate the container_ref
        container_ref = obj.get_container_ref()
        container_ref = generic.Reference(
            idnum=container_ref.idnum,
            generation=container_ref.generation,
            pdf=self,
        )

        if isinstance(obj, generic.DecryptedObjectProxy):
            raw_obj_replacement = self._subsume_object(obj.raw_object)
            # NOTE: we _can't_ decrypt the object here, that breaks
            #  in cases where there are descendants that are exempt from
            #  encryption (the Encrypt dictionary itself, signature contents,
            #  etc.).
            result = generic.DecryptedObjectProxy(
                raw_object=raw_obj_replacement, handler=obj.handler
            )
            raw_obj_replacement.container_ref = container_ref
            return result
        if isinstance(obj, generic.IndirectObject):
            # no container_ref necessary
            return generic.IndirectObject(
                idnum=obj.idnum, generation=obj.generation, pdf=self
            )
        elif isinstance(obj, generic.StreamObject):
            result = generic.StreamObject(
                {k: self._subsume_object(v) for k, v in obj.items()},
                encoded_data=obj.encoded_data,
            )
        elif isinstance(obj, generic.DictionaryObject):
            result = generic.DictionaryObject(
                {k: self._subsume_object(v) for k, v in obj.items()}
            )
        elif isinstance(obj, generic.ArrayObject):
            result = generic.ArrayObject(self._subsume_object(v) for v in obj)
        else:
            # in this case, we _never_ set the container_ref, and just
            # reuse the object from the "parent" reader.
            return obj
        result.container_ref = container_ref
        return result

    @property
    def root_ref(self) -> generic.Reference:
        ref: generic.IndirectObject = self.reader.trailer.raw_get(
            '/Root', revision=self.revision
        )
        return generic.Reference(
            idnum=ref.idnum, generation=ref.generation, pdf=self
        )

    def __call__(self, ref: generic.Reference):
        return self.get_object(ref)

    def explicit_refs_in_revision(self):
        return self.reader.xrefs.explicit_refs_in_revision(self.revision)

    def refs_freed_in_revision(self):
        return self.reader.xrefs.refs_freed_in_revision(self.revision)

    def object_streams_used(self):
        return self.reader.xrefs.object_streams_used_in(self.revision)

    def is_ref_available(self, ref: generic.Reference) -> bool:
        """
        Check if the reference in question was in scope for this revision.
        This call doesn't care about the specific semantics of free vs. used
        objects; it conservatively answers 'no' in any situation where
        the object ID _could_ have been assigned by the revision in question.

        :param ref:
            A reference object (usually one written to by a newer revision)
        :return:
            ``True`` if the reference is unassignable, ``False`` otherwise.
        """

        xref_cache = self.reader.xrefs
        meta = xref_cache.get_xref_container_info(self.revision)
        # the maximal object ID is (size - 1), so the reference is allocatable
        # as long as the ID is strictly less than meta.size
        return ref.idnum >= meta.size

    def collect_dependencies(
        self, obj: generic.PdfObject, since_revision=None
    ) -> Set[generic.Reference]:
        """
        Collect all indirect references used by an object and its descendants.

        :param obj:
            The object to inspect.
        :param since_revision:
            Optionally specify a revision number that tells the scanner to only
            include objects IDs that were added in that revision or later.

            .. warning::
                In particular, this means that the scanner will not recurse
                into older objects either.

        :return:
            A :class:`set` of :class:`~.generic.Reference` objects.
        """
        result_set: Set[generic.Reference] = set()
        self._collect_indirect_references(obj, result_set, since_revision)
        return result_set

    def _collect_indirect_references(self, obj, seen, since_revision=None):
        if isinstance(obj, generic.IndirectObject):
            ref = obj.reference
            if ref in seen:
                return
            xrefs = self.reader.xrefs
            relevant = (
                since_revision is None
                or xrefs.get_introducing_revision(ref) >= since_revision
            )
            if relevant:
                seen.add(ref)
            elif since_revision is not None:
                # do not recurse into objects that already existed before the
                # target revision, since we won't (shouldn't!) find any new refs
                # there.
                return
            obj = self(ref)
        if isinstance(obj, generic.DictionaryObject):
            for v in obj.values():
                self._collect_indirect_references(v, seen, since_revision)
        elif isinstance(obj, generic.ArrayObject):
            for v in obj:
                self._collect_indirect_references(v, seen, since_revision)

    def _get_usages_of_ref(self, ref: generic.Reference) -> Set[RawPdfPath]:
        cache = self._indirect_object_access_cache or {}
        try:
            return cache[ref]
        except KeyError:
            return set()

    def _load_reverse_xref_cache(self):
        if self._indirect_object_access_cache is not None:
            return

        collected = defaultdict(set)

        # internally, _compute_paths_to_refs works with singly linked lists
        # to avoid having to create & destroy lots of list objects
        # We flatten everything when we're done
        def _compute_paths_to_refs(
            obj,
            cur_path: misc.ConsList[Union[str, int]],
            seen_in_path: misc.ConsList[generic.Reference],
            *,
            is_page_tree,
            page_tree_objs,
            is_struct_tree,
            struct_tree_objs,
        ):
            # optimisation: page tree gets special treatment
            # to prevent unnecessary paths from being generated when the
            # tree is entered from the outside (e.g. by following /P on a form
            # field/widget annotation)
            # The structure tree is similarly special-cased.

            # Can we deal with this more systematically? The problem is that
            # there's no a priori way to state which path from an object
            # to the trailer is the "canonical" one. For page objects
            #  things are a bit more clear-cut, so we deal with those
            # separately.

            if isinstance(obj, generic.IndirectObject):
                obj_ref = obj.reference
                if obj_ref in seen_in_path:
                    return
                collected[obj_ref].add(cur_path)
                seen_in_path = seen_in_path.cons(obj_ref)
                obj = self(obj_ref)
                if not is_page_tree and obj_ref in page_tree_objs:
                    return
                if not is_struct_tree and obj_ref in struct_tree_objs:
                    return
            if isinstance(obj, generic.DictionaryObject):
                for k, v in obj.items():
                    # another hack to eliminate some spurious extra paths
                    # that don't convey any useful information
                    if k == '/Parent' or (is_struct_tree and k == '/P'):
                        continue

                    _compute_paths_to_refs(
                        v,
                        cur_path.cons(k),
                        seen_in_path,
                        is_page_tree=is_page_tree
                        or (
                            cur_path.head == '/Root'
                            and k == '/Pages'
                            and cur_path.tail == misc.ConsList.empty()
                        ),
                        page_tree_objs=page_tree_objs,
                        # for the struct tree: we definitely want to
                        # consider the /ParentTree as an "external" feature
                        # here, since it contains lots of references to
                        # structure elements that only exist for indexing
                        # purposes so they don't add any extra information
                        # (in the sense that recursing into them accomplishes
                        # nothing)
                        is_struct_tree=is_struct_tree
                        or (
                            cur_path.head == '/StructTreeRoot'
                            and k == '/K'
                            and cur_path.tail == misc.ConsList.sing('/Root')
                        ),
                        struct_tree_objs=struct_tree_objs,
                    )
            elif isinstance(obj, generic.ArrayObject):
                for ix, v in enumerate(obj):
                    _compute_paths_to_refs(
                        v,
                        cur_path.cons(ix),
                        seen_in_path,
                        is_page_tree=is_page_tree,
                        page_tree_objs=page_tree_objs,
                        is_struct_tree=is_struct_tree,
                        struct_tree_objs=struct_tree_objs,
                    )

        def _collect_page_tree_refs(pages_obj):
            for kid in pages_obj['/Kids']:
                # should always be true, but hey
                if isinstance(kid, generic.IndirectObject):
                    yield kid.reference
                kid = kid.get_object()
                if kid.get('/Type', None) == '/Pages':
                    yield from _collect_page_tree_refs(kid)

        def _collect_struct_tree_refs(struct_elem):
            try:
                children = struct_elem['/K']
            except KeyError:
                return

            # if there's only one child, /K need not be an array
            if not isinstance(children, generic.ArrayObject):
                children = (children,)

            for child in children:
                child_ref = None
                if isinstance(child, generic.IndirectObject):
                    child_ref = child.reference
                    child = child.get_object()

                # The /K entry can also refer to content items.
                # We don't care about those.
                if not isinstance(child, generic.DictionaryObject):
                    continue
                try:
                    if child['/Type'] != '/StructElem':
                        continue
                except KeyError:
                    # If the child doesn't have a /Type entry, /StructElem
                    # is the default (says so in the spec)
                    pass

                if child_ref is not None:
                    yield child_ref

                yield from _collect_struct_tree_refs(child)

        pages_ref = self.root.raw_get('/Pages')
        page_tree_nodes = set()
        for ref in _collect_page_tree_refs(pages_obj=pages_ref.get_object()):
            if ref in page_tree_nodes:
                raise misc.PdfReadError(
                    "Circular reference in page tree in mapping stage"
                )
            page_tree_nodes.add(ref)
        struct_tree_nodes = set()
        try:
            struct_tree_root_ref = self.root.raw_get('/StructTreeRoot')
            struct_tree_root = struct_tree_root_ref.get_object()
            for ref in _collect_struct_tree_refs(struct_tree_root):
                if ref in struct_tree_nodes:
                    raise misc.PdfReadError(
                        "Circular reference in structure tree in mapping stage"
                    )
                struct_tree_nodes.add(ref)
        except KeyError:
            pass
        _compute_paths_to_refs(
            self.trailer_view,
            misc.ConsList.empty(),
            misc.ConsList.empty(),
            is_page_tree=False,
            page_tree_objs=page_tree_nodes,
            is_struct_tree=False,
            struct_tree_objs=struct_tree_nodes,
        )

        self._indirect_object_access_cache = {
            ref: {RawPdfPath(*reversed(list(p))) for p in paths}
            for ref, paths in collected.items()
        }
