"""
This module defines pyHanko's high-level API entry points.
"""

from datetime import datetime
from typing import Optional

import tzlocal
from asn1crypto import cms

from pyhanko.pdf_utils import embed
from pyhanko.pdf_utils.writer import BasePdfFileWriter
from pyhanko.sign.fields import SigFieldSpec
from pyhanko.sign.general import SigningError
from pyhanko.sign.timestamps import TimeStamper

from .pdf_cms import Signer
from .pdf_signer import PdfSignatureMetadata, PdfSigner

__all__ = ['sign_pdf', 'async_sign_pdf', 'embed_payload_with_cms']


def sign_pdf(
    pdf_out: BasePdfFileWriter,
    signature_meta: PdfSignatureMetadata,
    signer: Signer,
    timestamper: Optional[TimeStamper] = None,
    new_field_spec: Optional[SigFieldSpec] = None,
    existing_fields_only=False,
    bytes_reserved=None,
    in_place=False,
    output=None,
):
    """
    Thin convenience wrapper around :meth:`.PdfSigner.sign_pdf`.

    :param pdf_out:
        An :class:`.IncrementalPdfFileWriter`.
    :param bytes_reserved:
        Bytes to reserve for the CMS object in the PDF file.
        If not specified, make an estimate based on a dummy signature.
    :param signature_meta:
        The specification of the signature to add.
    :param signer:
        :class:`.Signer` object to use to produce the signature object.
    :param timestamper:
        :class:`.TimeStamper` object to use to produce any time stamp tokens
        that might be required.
    :param in_place:
        Sign the input in-place. If ``False``, write output to a
        :class:`.BytesIO` object.
    :param existing_fields_only:
        If ``True``, never create a new empty signature field to contain
        the signature.
        If ``False``, a new field may be created if no field matching
        :attr:`~.PdfSignatureMetadata.field_name` exists.
    :param new_field_spec:
        If a new field is to be created, this parameter allows the caller
        to specify the field's properties in the form of a
        :class:`.SigFieldSpec`. This parameter is only meaningful if
        ``existing_fields_only`` is ``False``.
    :param output:
        Write the output to the specified output stream.
        If ``None``, write to a new :class:`.BytesIO` object.
        Default is ``None``.
    :return:
        The output stream containing the signed output.
    """

    if new_field_spec is not None and existing_fields_only:
        raise SigningError(
            "Specifying a signature field spec is not meaningful when "
            "existing_fields_only=True."
        )

    pdf_signer = PdfSigner(
        signature_meta,
        signer,
        timestamper=timestamper,
        new_field_spec=new_field_spec,
    )
    return pdf_signer.sign_pdf(
        pdf_out,
        existing_fields_only=existing_fields_only,
        bytes_reserved=bytes_reserved,
        in_place=in_place,
        output=output,
    )


async def async_sign_pdf(
    pdf_out: BasePdfFileWriter,
    signature_meta: PdfSignatureMetadata,
    signer: Signer,
    timestamper: Optional[TimeStamper] = None,
    new_field_spec: Optional[SigFieldSpec] = None,
    existing_fields_only=False,
    bytes_reserved=None,
    in_place=False,
    output=None,
):
    """
    Thin convenience wrapper around :meth:`.PdfSigner.async_sign_pdf`.

    :param pdf_out:
        An :class:`.IncrementalPdfFileWriter`.
    :param bytes_reserved:
        Bytes to reserve for the CMS object in the PDF file.
        If not specified, make an estimate based on a dummy signature.
    :param signature_meta:
        The specification of the signature to add.
    :param signer:
        :class:`.Signer` object to use to produce the signature object.
    :param timestamper:
        :class:`.TimeStamper` object to use to produce any time stamp tokens
        that might be required.
    :param in_place:
        Sign the input in-place. If ``False``, write output to a
        :class:`.BytesIO` object.
    :param existing_fields_only:
        If ``True``, never create a new empty signature field to contain
        the signature.
        If ``False``, a new field may be created if no field matching
        :attr:`~.PdfSignatureMetadata.field_name` exists.
    :param new_field_spec:
        If a new field is to be created, this parameter allows the caller
        to specify the field's properties in the form of a
        :class:`.SigFieldSpec`. This parameter is only meaningful if
        ``existing_fields_only`` is ``False``.
    :param output:
        Write the output to the specified output stream.
        If ``None``, write to a new :class:`.BytesIO` object.
        Default is ``None``.
    :return:
        The output stream containing the signed output.
    """

    if new_field_spec is not None and existing_fields_only:
        raise SigningError(
            "Specifying a signature field spec is not meaningful when "
            "existing_fields_only=True."
        )

    pdf_signer = PdfSigner(
        signature_meta,
        signer,
        timestamper=timestamper,
        new_field_spec=new_field_spec,
    )
    return await pdf_signer.async_sign_pdf(
        pdf_out,
        existing_fields_only=existing_fields_only,
        bytes_reserved=bytes_reserved,
        in_place=in_place,
        output=output,
    )


def embed_payload_with_cms(
    pdf_writer: BasePdfFileWriter,
    file_spec_string: str,
    payload: embed.EmbeddedFileObject,
    cms_obj: cms.ContentInfo,
    extension='.sig',
    file_name: Optional[str] = None,
    file_spec_kwargs=None,
    cms_file_spec_kwargs=None,
):
    """
    Embed some data as an embedded file stream into a PDF, and associate it
    with a CMS object.

    The resulting CMS object will also be turned into an embedded file, and
    associated with the original payload through a related file relationship.

    This can be used to bundle (non-PDF) detached signatures with PDF
    attachments, for example.

    .. versionadded:: 0.7.0

    :param pdf_writer:
        The PDF writer to use.
    :param file_spec_string:
        See :attr:`~pyhanko.pdf_utils.embed.FileSpec.file_spec_string` in
        :class:`~pyhanko.pdf_utils.embed.FileSpec`.
    :param payload:
        Payload object.
    :param cms_obj:
        CMS object pertaining to the payload.
    :param extension:
        File extension to use for the CMS attachment.
    :param file_name:
        See :attr:`~pyhanko.pdf_utils.embed.FileSpec.file_name` in
        :class:`~pyhanko.pdf_utils.embed.FileSpec`.
    :param file_spec_kwargs:
        Extra arguments to pass to the
        :class:`~pyhanko.pdf_utils.embed.FileSpec` constructor
        for the main attachment specification.
    :param cms_file_spec_kwargs:
        Extra arguments to pass to the
        :class:`~pyhanko.pdf_utils.embed.FileSpec` constructor
        for the CMS attachment specification.
    """

    # prepare an embedded file object for the signature
    now = datetime.now(tz=tzlocal.get_localzone())
    cms_ef_obj = embed.EmbeddedFileObject.from_file_data(
        pdf_writer=pdf_writer,
        data=cms_obj.dump(),
        compress=False,
        mime_type='application/pkcs7-mime',
        params=embed.EmbeddedFileParams(
            creation_date=now, modification_date=now
        ),
    )

    # replace extension
    cms_data_f = file_spec_string.rsplit('.', 1)[0] + extension

    # deal with new-style Unicode file names
    cms_data_uf = uf_related_files = None
    if file_name is not None:
        cms_data_uf = file_name.rsplit('.', 1)[0] + extension
        uf_related_files = [
            embed.RelatedFileSpec(cms_data_uf, embedded_data=cms_ef_obj)
        ]

    spec = embed.FileSpec(
        file_spec_string=file_spec_string,
        file_name=file_name,
        embedded_data=payload,
        f_related_files=[
            embed.RelatedFileSpec(cms_data_f, embedded_data=cms_ef_obj)
        ],
        uf_related_files=uf_related_files,
        **(file_spec_kwargs or {}),
    )

    embed.embed_file(pdf_writer, spec)

    # also embed the CMS data as a standalone attachment
    cms_spec = embed.FileSpec(
        file_spec_string=cms_data_f,
        file_name=cms_data_uf,
        embedded_data=cms_ef_obj,
        **(cms_file_spec_kwargs or {}),
    )
    embed.embed_file(pdf_writer, cms_spec)
