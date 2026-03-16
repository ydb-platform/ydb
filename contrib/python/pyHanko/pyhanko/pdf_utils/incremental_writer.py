"""
Utility for writing incremental updates to existing PDF files.
"""

import os
from typing import Optional, Union

from . import generic, misc
from .crypt import EnvelopeKeyDecrypter
from .generic import pdf_name
from .metadata.model import DocumentMetadata
from .reader import PdfFileReader, parse_catalog_version
from .writer import BasePdfFileWriter

__all__ = ['IncrementalPdfFileWriter']


class IncrementalPdfFileWriter(BasePdfFileWriter):
    """Class to incrementally update existing files.

    This :class:`~.writer.BasePdfFileWriter` subclass encapsulates a
    :class:`~.reader.PdfFileReader` instance in addition to exposing an
    interface to add and modify PDF objects.

    Incremental updates to a PDF file append modifications to the end of the
    file. This is critical when the original file contents are not to be
    modified directly (e.g. when it contains digital signatures).
    It has the additional advantage of providing an automatic audit trail of
    sorts.

    :param input_stream:
        Input stream to read current revision from.
    :param strict:
        Ingest the source file in strict mode. The default is ``True``.
    :param prev:
        Explicitly pass in a PDF reader. This parameter is internal API.
    """

    IO_CHUNK_SIZE = 4096
    _force_write_when_empty = False

    def __init__(
        self, input_stream, prev: Optional[PdfFileReader] = None, strict=True
    ):
        self.input_stream = input_stream
        if prev is None:
            prev = PdfFileReader(input_stream, strict=strict)
        self.prev = prev
        self.trailer = trailer = prev.trailer
        root_ref = trailer.raw_get('/Root')
        try:
            info_ref = trailer.raw_get('/Info')
        except KeyError:
            # rare, but it can happen. /Info is not a required entry
            info_ref = None
        document_id = self.__class__._handle_id(prev)
        super().__init__(
            root_ref,
            info_ref,
            document_id,
            obj_id_start=trailer['/Size'],
            stream_xrefs=prev.has_xref_stream,
        )
        if self._info is not None:
            self.trailer['/Info'] = self._info
        self._resolves_objs_from = (self, prev)
        input_ver = self.prev.input_version
        if input_ver > self.output_version:
            self.output_version = input_ver
        self.ensure_output_version(self.__class__.output_version)

        self.security_handler = prev.security_handler
        if self.security_handler is not None:
            self._encrypt = prev.trailer.raw_get("/Encrypt")

    @classmethod
    def from_reader(cls, reader: PdfFileReader) -> 'IncrementalPdfFileWriter':
        """
        Instantiate an incremental writer from a PDF file reader.

        :param reader:
            A :class:`.PdfFileReader` object with a PDF to extend.
        """

        return cls(reader.stream, prev=reader)

    def ensure_output_version(self, version):
        # check header
        if self.prev.input_version >= version:
            return
        # check root
        root = self.root
        try:
            ver = root[pdf_name('/Version')]
            cur_version = parse_catalog_version(ver)
            if cur_version is not None and cur_version >= version:
                return
        except (KeyError, ValueError, TypeError):
            pass
        version_str = pdf_name('/%d.%d' % version)
        root[pdf_name('/Version')] = version_str
        self.update_root()
        self.output_version = version

    @classmethod
    def _handle_id(cls, prev):
        # There are a number of issues at play here
        #  - Documents *should* have a unique id, but it's not a strict
        #    requirement unless the document is encrypted.
        #  - We are updating an existing document, but the result is not the
        #    same document. Hence, we want to assign an ID to this document that
        #    is not the same as the one on the existing document.
        #  - The first part of the ID is part of the key derivation used to
        #    to encrypt documents. Since we need to encrypt the file using
        #    the same cryptographic data as the original, we cannot change
        #    this value if it is present (cf. § 7.6.3.3 in ISO 32000).
        #    Even when no encryption is involved, changing this part violates
        #    the spec (cf. § 14.4 in loc. cit.)

        # noinspection PyArgumentList
        id2 = generic.ByteStringObject(os.urandom(16))
        try:
            id1, _ = prev.trailer["/ID"]
            # is this a bug in PyPDF2?
            if isinstance(id1, generic.TextStringObject):
                # noinspection PyArgumentList
                id1 = generic.ByteStringObject(id1.original_bytes)
        except KeyError:
            # no primary ID present, so generate one
            # noinspection PyArgumentList
            id1 = generic.ByteStringObject(os.urandom(16))
        return generic.ArrayObject([id1, id2])

    def get_object(self, ido, as_metadata_stream: bool = False):
        try:
            return super().get_object(
                ido, as_metadata_stream=as_metadata_stream
            )
        except KeyError:
            return self.prev.get_object(
                ido, as_metadata_stream=as_metadata_stream
            )

    def mark_update(
        self, obj_ref: Union[generic.Reference, generic.IndirectObject]
    ):
        ix = (obj_ref.generation, obj_ref.idnum)
        self.objects[ix] = obj_ref.get_object()

    # TODO: this new API allows me to simplify a lot of bookkeeping
    #  in the library
    def update_container(self, obj: generic.PdfObject):
        container_ref = obj.container_ref
        if container_ref is None:
            # this means that in all likelihood, the object was added by this
            # writer, and is therefore about to be written anyway.
            return
        if isinstance(container_ref, generic.TrailerReference):
            # nothing to do, the trailer is always written
            return
        elif isinstance(container_ref, generic.Reference):
            self.mark_update(container_ref)
            return
        raise TypeError  # pragma: nocover

    def update_root(self):
        self.mark_update(self._root)

    def _write_header(self, stream):
        # copy the original data to the output
        input_pos = self.input_stream.tell()
        self.input_stream.seek(0)
        misc.chunked_write(
            bytearray(self.IO_CHUNK_SIZE), self.input_stream, stream
        )
        self.input_stream.seek(input_pos)

    def set_info(
        self,
        info: Optional[Union[generic.IndirectObject, generic.DictionaryObject]],
    ):
        info = super().set_info(info)
        if info is not None:
            # also update our trailer
            self.trailer['/Info'] = info
        else:
            del self.trailer['/Info']
        return info

    def _populate_trailer(self, trailer):
        trailer.update(self.trailer.flatten())
        super()._populate_trailer(trailer)
        trailer[pdf_name('/Prev')] = generic.NumberObject(
            self.prev.last_startxref
        )
        if self.prev.encrypted and not self.security_handler.is_authenticated():
            # removing encryption in an incremental update is impossible
            raise misc.PdfWriteError(
                'Cannot update this document without encryption credentials '
                'from the original. Please call encrypt() with the password '
                'of the original file before calling write().'
            )

    def set_custom_trailer_entry(
        self, key: generic.NameObject, value: generic.PdfObject
    ):
        """
        Set a custom, unmanaged entry in the document trailer or cross-reference
        stream dictionary.

        .. warning::
            Calling this method to set an entry that is managed by pyHanko
            internally (info dictionary, document catalog, etc.) has undefined
            results.

        :param key:
            Dictionary key to use in the trailer.
        :param value:
            Value to set
        """
        self.trailer[key] = value

    def write(self, stream):
        if not self.objects and not self._force_write_when_empty:
            # just write the original and then bail
            self._write_header(stream)
            return
        self._prep_dom_for_writing()
        super().write(stream)

    @property
    def document_meta_view(self) -> DocumentMetadata:
        return self._meta.view_over(self.prev.document_meta_view)

    def _write_updated_section(self, stream):
        """
        Only write the updated and new objects to the designated output stream.

        The new PDF file can then be put together by concatenating the original
        input with the generated output.

        .. danger::
            The offsets in this output will typically be wrong unless the missing
            previous section is somehow taken into account.

        .. danger::
            Object graph finalisation will not run.

        :param stream:
            Output stream to write to.
        """
        self._write(stream, skip_header=True)

    def write_in_place(self):
        """
        Write the updated file contents in-place to the same stream as
        the input stream.
        This obviously requires a stream supporting both reading and writing
        operations.
        """
        self._prep_dom_for_writing()
        if self._need_mac_on_write:
            from pyhanko.pdf_utils.crypt import pdfmac

            pdfmac.add_standalone_mac(self, in_place=True)
        else:
            stream = self.prev.stream
            stream.seek(0, os.SEEK_END)
            self._write_updated_section(stream)

    def encrypt(self, user_pwd):
        """Method to handle updates to encrypted files.

        This method handles decrypting of the original file, and makes sure
        the resulting updated file is encrypted in a compatible way.
        The standard mandates that updates to encrypted files be effected using
        the same encryption settings. In particular, incremental updates
        cannot remove file encryption.

        :param user_pwd:
            The original file's user password.

        :raises PdfReadError:
            Raised when there is a problem decrypting the file.
        """

        prev = self.prev
        result = prev.decrypt(user_pwd)

        # take care to use the same encryption algorithm as the underlying file
        self._encrypt = prev.trailer.raw_get("/Encrypt")
        return result

    def encrypt_pubkey(self, credential: EnvelopeKeyDecrypter):
        """Method to handle updates to files encrypted using public-key
        encryption.

        The same caveats as :meth:`encrypt` apply here.


        :param credential:
            The :class:`.EnvelopeKeyDecrypter` handling the recipient's
            private key.

        :raises PdfReadError:
            Raised when there is a problem decrypting the file.
        """

        prev = self.prev
        result = prev.decrypt_pubkey(credential)
        self._encrypt = prev.trailer.raw_get("/Encrypt")
        return result
