"""
Utility classes for handling embedded files in PDFs.

.. versionadded:: 0.7.0
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from asn1crypto import x509

from . import crypt, generic, misc, writer
from .crypt.pubkey import RecipientEncryptionPolicy
from .font.basic import get_courier
from .generic import pdf_name, pdf_string

__all__ = [
    'embed_file',
    'EmbeddedFileObject',
    'EmbeddedFileParams',
    'FileSpec',
    'RelatedFileSpec',
    'wrap_encrypted_payload',
]


@dataclass(frozen=True)
class EmbeddedFileParams:
    embed_size: bool = True
    """
    If true, record the file size of the embedded file.
    
    .. note::
        This value is computed over the file content before PDF filters
        are applied. This may have performance implications in cases where the
        file stream contents are presented in pre-encoded form.
    """

    embed_checksum: bool = True
    """
    If true, add an MD5 checksum of the file contents.

    .. note::
        This value is computed over the file content before PDF filters
        are applied. This may have performance implications in cases where the
        file stream contents are presented in pre-encoded form.
    """

    creation_date: Optional[datetime] = None
    """
    Record the creation date of the embedded file.
    """

    modification_date: Optional[datetime] = None
    """
    Record the modification date of the embedded file.
    """


class EmbeddedFileObject(generic.StreamObject):
    @classmethod
    def from_file_data(
        cls,
        pdf_writer: writer.BasePdfFileWriter,
        data: bytes,
        compress=True,
        params: Optional[EmbeddedFileParams] = None,
        mime_type: Optional[str] = None,
    ) -> 'EmbeddedFileObject':
        """
        Construct an embedded file object from file data.

        This is a very thin wrapper around the constructor, with a slightly
        less intimidating API.

        .. note::
            This method will not register the embedded file into the document's
            embedded file namespace, see :func:`.embed_file`.

        :param pdf_writer:
            PDF writer to use.
        :param data:
            File contents, as a :class:`bytes` object.
        :param compress:
            Whether to compress the embedded file's contents.
        :param params:
            Optional embedded file parameters.
        :param mime_type:
            Optional MIME type string.
        :return:
            An embedded file object.
        """

        result = EmbeddedFileObject(
            pdf_writer=pdf_writer,
            stream_data=data,
            params=params,
            mime_type=mime_type,
        )
        if compress:
            result.compress()

        return result

    def __init__(
        self,
        pdf_writer: writer.BasePdfFileWriter,
        dict_data=None,
        stream_data=None,
        encoded_data=None,
        params: Optional[EmbeddedFileParams] = None,
        mime_type: Optional[str] = None,
    ):
        super().__init__(
            dict_data=dict_data,
            stream_data=stream_data,
            encoded_data=encoded_data,
            handler=pdf_writer.security_handler,
        )
        self['/Type'] = generic.pdf_name('/EmbeddedFile')
        if mime_type is not None:
            self['/Subtype'] = generic.pdf_name('/' + mime_type)
        self.ef_stream_ref = pdf_writer.add_object(self)
        self.params = params

    def write_to_stream(self, stream, handler=None, container_ref=None):
        # deal with "encrypt embedded files only" mode
        # (we do this here to make sure the user doesn't add any other crypt
        # filters after this one)
        if handler is not None and not self._has_crypt_filter:
            cfc = handler.crypt_filter_config
            ef_filter_name = cfc.embedded_file_filter_name
            stream_filter_name = cfc.stream_filter_name
            if (
                ef_filter_name is not None
                and ef_filter_name != stream_filter_name
            ):
                self.add_crypt_filter(ef_filter_name)

        # apply the parameters before serialisation
        params = self.params
        if params is not None:
            self['/Params'] = param_dict = generic.DictionaryObject()
            if params.embed_size:
                param_dict['/Size'] = generic.NumberObject(len(self.data))
            if params.embed_checksum:
                checksum = hashlib.md5(self.data).digest()
                param_dict['/CheckSum'] = generic.ByteStringObject(checksum)
            if params.creation_date is not None:
                param_dict['/CreationDate'] = generic.pdf_date(
                    params.creation_date
                )
            if params.modification_date is not None:
                param_dict['/ModDate'] = generic.pdf_date(
                    params.modification_date
                )

        super().write_to_stream(
            stream, handler=handler, container_ref=container_ref
        )


@dataclass(frozen=True)
class RelatedFileSpec:
    """
    Dataclass modelling a RelatedFile construct in PDF.
    """

    name: str
    """
    Name of the related file.
    
    .. note::
        The encoding requirements of this field depend on whether the related
        file is included via the ``/F`` or ``/UF`` key.
    """

    embedded_data: EmbeddedFileObject
    """
    Reference to a stream object containing the file's data, as embedded
    in the PDF file.
    """

    @classmethod
    def fmt_related_files(cls, lst: List['RelatedFileSpec']):
        def _gen():
            for rfs in lst:
                yield generic.pdf_string(rfs.name)
                yield rfs.embedded_data.ef_stream_ref

        return generic.ArrayObject(_gen())


@dataclass(frozen=True)
class FileSpec:
    """
    Dataclass modelling an embedded file description in a PDF.
    """

    # TODO collection item dictionaries

    # TODO thumbnail support

    # TODO enforce PDFDocEncoding for file_spec_string etc.

    file_spec_string: str
    """
    A path-like file specification string, or URL.
    
    .. note::
        For backwards compatibility, this string should be encodable in
        PDFDocEncoding. For names that require general Unicode support, refer
        to :class:`file_name`.
    """

    file_name: Optional[str] = None
    """
    A path-like Unicode file name.
    """

    embedded_data: Optional[EmbeddedFileObject] = None
    """
    Reference to a stream object containing the file's data, as embedded
    in the PDF file.
    """

    description: Optional[str] = None
    """
    Textual description of the file.
    """

    af_relationship: Optional[generic.NameObject] = None
    """
    Associated file relationship specifier.
    """

    f_related_files: Optional[List[RelatedFileSpec]] = None
    """
    Related files with PDFDocEncoded names.
    """

    uf_related_files: Optional[List[RelatedFileSpec]] = None
    """
    Related files with Unicode-encoded names.
    """

    def as_pdf_object(self) -> generic.DictionaryObject:
        """
        Represent the file spec as a PDF dictionary.
        """

        result = generic.DictionaryObject(
            {
                pdf_name('/Type'): pdf_name('/Filespec'),
                pdf_name('/F'): pdf_string(self.file_spec_string),
            }
        )
        if self.file_name is not None:
            result['/UF'] = pdf_string(self.file_name)

        if self.embedded_data is not None:
            result['/EF'] = ef_dict = generic.DictionaryObject(
                {
                    pdf_name('/F'): self.embedded_data.ef_stream_ref,
                }
            )
            if self.file_name is not None:
                ef_dict['/UF'] = self.embedded_data.ef_stream_ref

        if self.description is not None:
            result['/Desc'] = generic.TextStringObject(self.description)

        if self.af_relationship is not None:
            result['/AFRelationship'] = self.af_relationship

        f_related = self.f_related_files
        uf_related = self.uf_related_files
        if f_related or uf_related:
            result['/RF'] = rf = generic.DictionaryObject()
            if f_related:
                rf['/F'] = RelatedFileSpec.fmt_related_files(f_related)
            if uf_related and self.file_name is not None:
                rf['/UF'] = RelatedFileSpec.fmt_related_files(uf_related)

        return result


def embed_file(pdf_writer: writer.BasePdfFileWriter, spec: FileSpec):
    """
    Embed a file in the document-wide embedded file registry of a PDF writer.

    :param pdf_writer:
        PDF writer to house the embedded file.
    :param spec:
        File spec describing the embedded file.
    :return:
    """

    ef_stream = spec.embedded_data

    if ef_stream is None:
        raise misc.PdfWriteError(
            "File spec does not have an embedded file stream"
        )

    spec_obj = spec.as_pdf_object()

    root = pdf_writer.root
    try:
        names_dict = root['/Names']
    except KeyError:
        names_dict = generic.DictionaryObject()
        root['/Names'] = pdf_writer.add_object(names_dict)
        pdf_writer.update_root()

    try:
        ef_name_tree = names_dict['/EmbeddedFiles']
    except KeyError:
        ef_name_tree = generic.DictionaryObject()
        names_dict['/EmbeddedFiles'] = pdf_writer.add_object(ef_name_tree)
        pdf_writer.update_container(names_dict)

    # TODO support updating general name trees!
    #  (should probably be refactored into an utility method somewhere)
    if '/Kids' in ef_name_tree:
        raise NotImplementedError(
            "Only flat name trees are supported right now"
        )

    try:
        ef_name_arr = ef_name_tree['/Names']
    except KeyError:
        ef_name_arr = generic.ArrayObject()
        ef_name_tree['/Names'] = pdf_writer.add_object(ef_name_arr)
        pdf_writer.update_container(ef_name_tree)

    ef_name_arr.append(generic.pdf_string(spec.file_spec_string))
    spec_obj_ref = pdf_writer.add_object(spec_obj)
    ef_name_arr.append(spec_obj_ref)
    pdf_writer.update_container(ef_name_arr)

    if spec.af_relationship is not None:
        pdf_writer.ensure_output_version(version=(2, 0))
        # add the filespec to the /AF entry in the document catalog
        # TODO allow associations with objects other than the catalog?
        try:
            root_af_arr = root['/AF']
        except KeyError:
            root_af_arr = generic.ArrayObject()
            root['/AF'] = pdf_writer.add_object(root_af_arr)
            pdf_writer.update_root()
        root_af_arr.append(spec_obj_ref)
    else:
        pdf_writer.ensure_output_version(version=(1, 7))


def wrap_encrypted_payload(
    plaintext_payload: bytes,
    *,
    password: Optional[str] = None,
    certs: Optional[List[x509.Certificate]] = None,
    security_handler: Optional[crypt.SecurityHandler] = None,
    file_spec_string: str = 'attachment.pdf',
    params: Optional[EmbeddedFileParams] = None,
    file_name: Optional[str] = None,
    description='Wrapped document',
    include_explanation_page=True,
) -> writer.PdfFileWriter:
    """
    Include a PDF document as an encrypted attachment in a wrapper document.

    This function sets certain flags in the wrapper document's collection
    dictionary to instruct compliant PDF viewers to display the attachment
    instead of the wrapping document. Viewers that do not fully support
    PDF collections will display a landing page instead, explaining
    how to open the attachment manually.

    Using this method mitigates some weaknesses in the PDF standard's encryption
    provisions, and makes it harder to manipulate the encrypted attachment
    without knowing the encryption key.

    .. danger::
        Until PDF supports authenticated encryption mechanisms, this is
        a mitigation strategy, not a foolproof defence mechanism.

    .. warning::
        While users of viewers that do not support PDF collections can still
        open the attached file manually, the viewer still has to support
        PDF files where only the attachments are encrypted.

    .. note::
        This is not quite the same as the "unencrypted wrapper document"
        pattern discussed in the PDF 2.0 specification. The latter is intended
        to support nonstandard security handlers. This function uses a standard
        security handler on the wrapping document to encrypt the attachment
        as a binary blob.
        Moreover, the functionality in this function is available in PDF 1.7
        viewers as well.

    :param plaintext_payload:
        The plaintext payload (a binary representation of a PDF document).
    :param security_handler:
        The security handler to use on the wrapper document.
        If ``None``, a security handler will be constructed based on the
        ``password`` or ``certs`` parameter.
    :param password:
        Password to encrypt the attachment with.
        Will be ignored if ``security_handler`` is provided.
    :param certs:
        Encrypt the file using PDF public-key encryption, targeting the
        keys in the provided certificates.
        Will be ignored if ``security_handler`` is provided.
    :param file_spec_string:
        PDFDocEncoded file spec string for the attachment.
    :param params:
        Embedded file parameters to use.
    :param file_name:
        Unicode file name for the attachment.
    :param description:
        Description for the attachment
    :param include_explanation_page:
        If ``False``, do not generate an explanation page in the wrapper
        document. This setting could be useful if you want to customise the
        wrapper document's behaviour yourself.
    :return:
        A :class:`~writer.PdfFileWriter` representing the wrapper document.
    """
    w = writer.PdfFileWriter()

    if security_handler is None:
        if (password is None) == (certs is None):
            raise ValueError(
                "If 'security_handler' is not provided, "
                "exactly one of 'password' or 'cert' must be."
            )
        if password is None:
            assert certs is not None
            # set up pubkey security handler
            pubkey_cf = crypt.PubKeyAESCryptFilter(
                keylen=32, acts_as_default=False, encrypt_metadata=False
            )
            pubkey_cf.set_embedded_only()
            security_handler = crypt.PubKeySecurityHandler(
                version=crypt.SecurityHandlerVersion.AES256,
                pubkey_handler_subfilter=crypt.PubKeyAdbeSubFilter.S5,
                legacy_keylen=None,
                encrypt_metadata=False,
                crypt_filter_config=crypt.CryptFilterConfiguration(
                    {crypt.DEF_EMBEDDED_FILE: pubkey_cf},
                    default_file_filter=crypt.DEF_EMBEDDED_FILE,
                ),
            )
            pubkey_cf.add_recipients(certs, policy=RecipientEncryptionPolicy())
        else:
            # set up standard security handler
            std_cf = crypt.StandardAESCryptFilter(keylen=32)
            std_cf.set_embedded_only()
            security_handler = crypt.StandardSecurityHandler.build_from_pw(
                password,
                crypt_filter_config=crypt.CryptFilterConfiguration(
                    {crypt.STD_CF: std_cf}, default_file_filter=crypt.STD_CF
                ),
                encrypt_metadata=False,
            )
    w._assign_security_handler(security_handler)

    w.root['/Collection'] = collection_dict = generic.DictionaryObject()
    collection_dict['/Type'] = pdf_name('/Collection')
    collection_dict['/D'] = generic.TextStringObject(file_spec_string)
    collection_dict['/View'] = pdf_name('/H')  # hide "Collection" view

    ef_obj = EmbeddedFileObject.from_file_data(
        w,
        data=plaintext_payload,
        mime_type='application/pdf',
        params=params or EmbeddedFileParams(),
    )

    spec = FileSpec(
        file_spec_string=file_spec_string,
        file_name=file_name,
        embedded_data=ef_obj,
        description=description,
    )
    embed_file(w, spec)

    if include_explanation_page:
        resources = generic.DictionaryObject(
            {
                pdf_name('/Font'): generic.DictionaryObject(
                    {pdf_name('/F1'): get_courier(w)}
                )
            }
        )

        # TODO make it easy to customise this
        #  (i.e. don't require the user to put together a page object by
        #   themselves)
        stream_content = '''
        BT
            /F1 10 Tf 10 830 Td 12 TL
            (This document is a wrapper for an encrypted attachment.) '
            (Your viewer should prompt for a password and ) '
            (open the attached file automatically.) Tj
            (If not, navigate to the attached document manually.) ' T*
            (In addition, your viewer must support encryption ) '
            (scoped to embedded files.) Tj
        ET
        '''

        stream = generic.StreamObject(
            stream_data=stream_content.encode('latin1')
        )
        explanation_page = writer.PageObject(
            contents=w.add_object(stream),
            # A4 media box
            media_box=(0, 0, 595.28, 841.89),
            resources=resources,
        )

        w.insert_page(explanation_page)

    return w
