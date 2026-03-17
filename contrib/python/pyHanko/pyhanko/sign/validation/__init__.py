import asyncio
import warnings
from typing import IO, Optional, TypeVar, Union

from asn1crypto import cms
from pyhanko_certvalidator import ValidationContext

from pyhanko.pdf_utils import misc

from ..diff_analysis import DiffPolicy
from .dss import (
    VRI,
    DocumentSecurityStore,
    async_add_validation_info,
    collect_validation_info,
)
from .errors import SigSeedValueValidationError, ValidationInfoReadingError
from .generic_cms import (
    async_validate_cms_signature,
    async_validate_detached_cms,
)
from .ltv import (
    RevocationInfoValidationType,
    apply_adobe_revocation_info,
    async_validate_pdf_ltv_signature,
    get_timestamp_chain,
)
from .pdf_embedded import (
    DocMDPInfo,
    EmbeddedPdfSignature,
    async_validate_pdf_signature,
    async_validate_pdf_timestamp,
    read_certification_data,
)
from .settings import KeyUsageConstraints
from .status import (
    DocumentTimestampStatus,
    ModificationInfo,
    PdfSignatureStatus,
    SignatureCoverageLevel,
    SignatureStatus,
    StandardCMSSignatureStatus,
)

__all__ = [
    'SignatureCoverageLevel',
    'PdfSignatureStatus',
    'DocumentTimestampStatus',
    'StandardCMSSignatureStatus',
    'ModificationInfo',
    'EmbeddedPdfSignature',
    'DocMDPInfo',
    'RevocationInfoValidationType',
    'VRI',
    'DocumentSecurityStore',
    'apply_adobe_revocation_info',
    'get_timestamp_chain',
    'read_certification_data',
    'validate_pdf_signature',
    'async_validate_pdf_signature',
    'validate_cms_signature',
    'async_validate_cms_signature',
    'validate_detached_cms',
    'async_validate_detached_cms',
    'validate_pdf_timestamp',
    'async_validate_pdf_timestamp',
    'validate_pdf_ltv_signature',
    'async_validate_pdf_ltv_signature',
    'collect_validation_info',
    'add_validation_info',
]


StatusType = TypeVar('StatusType', bound=SignatureStatus)


# noinspection PyUnusedLocal
def validate_cms_signature(
    signed_data: cms.SignedData,
    status_cls=SignatureStatus,
    raw_digest: Optional[bytes] = None,
    validation_context: Optional[ValidationContext] = None,
    status_kwargs: Optional[dict] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    encap_data_invalid=False,
):
    """
    .. deprecated:: 0.9.0
        Use :func:`~.generic_cms.async_validate_cms_signature` instead.

    .. versionchanged:: 0.7.0
        Now handles both detached and enveloping signatures.

    .. versionchanged:: 0.17.0
        The ``encap_data_invalid`` parameter is ignored.

    Validate a CMS signature (i.e. a ``SignedData`` object).

    :param signed_data:
        The :class:`.asn1crypto.cms.SignedData` object to validate.
    :param status_cls:
        Status class to use for the validation result.
    :param raw_digest:
        Raw digest, computed from context.
    :param validation_context:
        Validation context to validate the signer's certificate.
    :param status_kwargs:
        Other keyword arguments to pass to the ``status_class`` when reporting
        validation results.
    :param key_usage_settings:
        A :class:`.KeyUsageConstraints` object specifying which key usages
        must or must not be present in the signer's certificate.
    :param encap_data_invalid:
        As of version ``0.17.0``, this parameter is ignored.
    :return:
        A :class:`.SignatureStatus` object (or an instance of a proper subclass)
    """

    warnings.warn(
        "'validate_cms_signature' is deprecated, use "
        "'async_validate_cms_signature' instead",
        DeprecationWarning,
    )

    coro = async_validate_cms_signature(
        signed_data=signed_data,
        status_cls=status_cls,
        raw_digest=raw_digest,
        validation_context=validation_context,
        status_kwargs=status_kwargs,
        key_usage_settings=key_usage_settings,
    )
    return asyncio.run(coro)


def validate_detached_cms(
    input_data: Union[bytes, IO, cms.ContentInfo, cms.EncapsulatedContentInfo],
    signed_data: cms.SignedData,
    signer_validation_context: Optional[ValidationContext] = None,
    ts_validation_context: Optional[ValidationContext] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    chunk_size=misc.DEFAULT_CHUNK_SIZE,
    max_read=None,
) -> StandardCMSSignatureStatus:
    """
    .. deprecated:: 0.9.0
        Use :func:`.generic_cms.async_validate_detached_cms` instead.

    .. versionadded: 0.7.0

    Validate a detached CMS signature.

    :param input_data:
        The input data to sign. This can be either a :class:`bytes` object,
        a file-like object or a :class:`cms.ContentInfo` /
        :class:`cms.EncapsulatedContentInfo` object.

        If a CMS content info object is passed in, the `content` field
        will be extracted.
    :param signed_data:
        The :class:`cms.SignedData` object containing the signature to verify.
    :param signer_validation_context:
        Validation context to use to verify the signer certificate's trust.
    :param ts_validation_context:
        Validation context to use to verify the TSA certificate's trust, if
        a timestamp token is present.
        By default, the same validation context as that of the signer is used.
    :param key_usage_settings:
        Key usage parameters for the signer.
    :param chunk_size:
        Chunk size to use when consuming input data.
    :param max_read:
        Maximal number of bytes to read from the input stream.
    :return:
        A description of the signature's status.
    """

    warnings.warn(
        "'validate_detached_cms' is deprecated, use "
        "'async_validate_detached_cms' instead",
        DeprecationWarning,
    )

    coro = async_validate_detached_cms(
        input_data=input_data,
        signed_data=signed_data,
        signer_validation_context=signer_validation_context,
        ts_validation_context=ts_validation_context,
        key_usage_settings=key_usage_settings,
        chunk_size=chunk_size,
        max_read=max_read,
    )
    return asyncio.run(coro)


def validate_pdf_signature(
    embedded_sig: EmbeddedPdfSignature,
    signer_validation_context: Optional[ValidationContext] = None,
    ts_validation_context: Optional[ValidationContext] = None,
    diff_policy: Optional[DiffPolicy] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    skip_diff: bool = False,
) -> PdfSignatureStatus:
    """
    .. versionchanged:: 0.9.0
        Wrapper around :func:`~.pdf_embedded.async_validate_pdf_signature`.

    Validate a PDF signature.

    :param embedded_sig:
        Embedded signature to evaluate.
    :param signer_validation_context:
        Validation context to use to validate the signature's chain of trust.
    :param ts_validation_context:
        Validation context to use to validate the timestamp's chain of trust
        (defaults to ``signer_validation_context``).
    :param diff_policy:
        Policy to evaluate potential incremental updates that were appended
        to the signed revision of the document.
        Defaults to
        :const:`~pyhanko.sign.diff_analysis.DEFAULT_DIFF_POLICY`.
    :param key_usage_settings:
        A :class:`.KeyUsageConstraints` object specifying which key usages
        must or must not be present in the signer's certificate.
    :param skip_diff:
        If ``True``, skip the difference analysis step entirely.
    :return:
        The status of the PDF signature in question.
    """
    coro = async_validate_pdf_signature(
        embedded_sig=embedded_sig,
        signer_validation_context=signer_validation_context,
        ts_validation_context=ts_validation_context,
        diff_policy=diff_policy,
        key_usage_settings=key_usage_settings,
        skip_diff=skip_diff,
    )
    return asyncio.run(coro)


def validate_pdf_timestamp(
    embedded_sig: EmbeddedPdfSignature,
    validation_context: Optional[ValidationContext] = None,
    diff_policy: Optional[DiffPolicy] = None,
    skip_diff: bool = False,
) -> DocumentTimestampStatus:
    """
    .. versionchanged:: 0.9.0
        Wrapper around :func:`~.pdf_embedded.async_validate_pdf_timestamp`.

    Validate a PDF document timestamp.

    :param embedded_sig:
        Embedded signature to evaluate.
    :param validation_context:
        Validation context to use to validate the timestamp's chain of trust.
    :param diff_policy:
        Policy to evaluate potential incremental updates that were appended
        to the signed revision of the document.
        Defaults to
        :const:`~pyhanko.sign.diff_analysis.DEFAULT_DIFF_POLICY`.
    :param skip_diff:
        If ``True``, skip the difference analysis step entirely.
    :return:
        The status of the PDF timestamp in question.
    """
    coro = async_validate_pdf_timestamp(
        embedded_sig=embedded_sig,
        validation_context=validation_context,
        diff_policy=diff_policy,
        skip_diff=skip_diff,
    )
    return asyncio.run(coro)


def add_validation_info(
    embedded_sig: EmbeddedPdfSignature,
    validation_context: ValidationContext,
    skip_timestamp=False,
    add_vri_entry=True,
    in_place=False,
    output=None,
    force_write=False,
    chunk_size=misc.DEFAULT_CHUNK_SIZE,
):
    """
    .. versionchanged:: 0.9.0
        Wrapper around :func:`~.dss.async_add_validation_info`

    Add validation info (CRLs, OCSP responses, extra certificates) for a
    signature to the DSS of a document in an incremental update.

    :param embedded_sig:
        The signature for which the revocation information needs to be
        collected.
    :param validation_context:
        The validation context to use.
    :param skip_timestamp:
        If ``True``, do not attempt to validate the timestamp attached to
        the signature, if one is present.
    :param add_vri_entry:
        Add a ``/VRI`` entry for this signature to the document security store.
        Default is ``True``.
    :param output:
        Write the output to the specified output stream.
        If ``None``, write to a new :class:`.BytesIO` object.
        Default is ``None``.
    :param in_place:
        Sign the original input stream in-place.
        This parameter overrides ``output``.
    :param chunk_size:
        Chunk size parameter to use when copying output to a new stream
        (irrelevant if ``in_place`` is ``True``).
    :param force_write:
        Force a new revision to be written, even if not necessary (i.e.
        when all data in the validation context is already present in the DSS).
    :return:
        The (file-like) output object to which the result was written.
    """

    coro = async_add_validation_info(
        embedded_sig=embedded_sig,
        validation_context=validation_context,
        skip_timestamp=skip_timestamp,
        add_vri_entry=add_vri_entry,
        output=output,
        in_place=in_place,
        chunk_size=chunk_size,
        force_write=force_write,
    )
    return asyncio.run(coro)


def validate_pdf_ltv_signature(
    embedded_sig: EmbeddedPdfSignature,
    validation_type: RevocationInfoValidationType,
    validation_context_kwargs=None,
    bootstrap_validation_context=None,
    force_revinfo=False,
    diff_policy: Optional[DiffPolicy] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    skip_diff: bool = False,
) -> PdfSignatureStatus:
    """
    .. versionchanged:: 0.9.0
        Wrapper around :func:`async_validate_pdf_ltv_signature`.

    Validate a PDF LTV signature according to a particular profile.

    :param embedded_sig:
        Embedded signature to evaluate.
    :param validation_type:
        Validation profile to use.
    :param validation_context_kwargs:
        Keyword args to instantiate
        :class:`.pyhanko_certvalidator.ValidationContext` objects needed over
        the course of the validation.
    :param bootstrap_validation_context:
        Validation context used to validate the current timestamp.
    :param force_revinfo:
        Require all certificates encountered to have some form of live
        revocation checking provisions.
    :param diff_policy:
        Policy to evaluate potential incremental updates that were appended
        to the signed revision of the document.
        Defaults to
        :const:`~pyhanko.sign.diff_analysis.DEFAULT_DIFF_POLICY`.
    :param key_usage_settings:
        A :class:`.KeyUsageConstraints` object specifying which key usages
        must or must not be present in the signer's certificate.
    :param skip_diff:
        If ``True``, skip the difference analysis step entirely.
    :return:
        The status of the signature.
    """
    coro = async_validate_pdf_ltv_signature(
        embedded_sig=embedded_sig,
        validation_type=validation_type,
        validation_context_kwargs=validation_context_kwargs,
        bootstrap_validation_context=bootstrap_validation_context,
        force_revinfo=force_revinfo,
        diff_policy=diff_policy,
        key_usage_settings=key_usage_settings,
        skip_diff=skip_diff,
    )
    return asyncio.run(coro)
