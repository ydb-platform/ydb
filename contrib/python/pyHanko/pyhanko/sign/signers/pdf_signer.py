"""
This module implements support for PDF-specific signing functionality.
"""

import asyncio
import enum
import logging
import uuid
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from typing import IO, Any, Dict, Iterable, List, Optional, Set, Tuple, Union

import tzlocal
from asn1crypto import algos, cms, crl, keys, ocsp
from asn1crypto import pdf as asn1_pdf
from asn1crypto.core import VOID
from cryptography.hazmat.primitives import hashes
from pyhanko_certvalidator import CertificateValidator, ValidationContext
from pyhanko_certvalidator.errors import PathBuildingError, PathValidationError
from pyhanko_certvalidator.path import ValidationPath
from pyhanko_certvalidator.validate import ACValidationResult, async_validate_ac

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.crypt import pdfmac
from pyhanko.pdf_utils.generic import pdf_name
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.pdf_utils.writer import BasePdfFileWriter
from pyhanko.sign import attributes
from pyhanko.sign.ades.api import CAdESSignedAttrSpec
from pyhanko.sign.fields import (
    FieldMDPSpec,
    InvisSigSettings,
    MDPPerm,
    SeedLockDocument,
    SigAuthType,
    SigFieldSpec,
    SigSeedSubFilter,
    SigSeedValFlags,
    SigSeedValueSpec,
    enumerate_sig_fields,
)
from pyhanko.sign.general import (
    SigningError,
    find_unique_cms_attribute,
    get_cms_hash_algo_for_mechanism,
    get_pyca_cryptography_hash,
    simple_cms_attribute,
)
from pyhanko.sign.timestamps import TimeStamper
from pyhanko.stamp import BaseStampStyle

from . import constants
from .cms_embedder import (
    PdfCMSEmbedder,
    SigAppearanceSetup,
    SigIOSetup,
    SigMDPSetup,
    SigObjSetup,
)
from .pdf_byterange import (
    BuildProps,
    DocumentTimestamp,
    PreparedByteRangeDigest,
    SignatureObject,
)
from .pdf_cms import PdfCMSSignedAttributes, Signer, select_suitable_signing_md

__all__ = [
    'PdfSignatureMetadata',
    'DSSContentSettings',
    'TimestampDSSContentSettings',
    'GeneralDSSContentSettings',
    'SigDSSPlacementPreference',
    'PdfTimeStamper',
    'PdfSigner',
    'PdfSigningSession',
    'PdfTBSDocument',
    'PdfPostSignatureDocument',
    'PreSignValidationStatus',
    'PostSignInstructions',
]

from ...pdf_utils.crypt import SerialisedCredential
from ..attributes import CMSAttributeProvider

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class GeneralDSSContentSettings:
    """
    .. versionadded:: 0.8.0

    Settings that govern DSS creation and updating in general.
    """

    include_vri: bool = True
    """
    Flag to control whether to create and update entries in the VRI dictionary.
    The default is to always update the VRI dictionary.
    
    .. note::
        The VRI dictionary is a relic of the past that is effectively
        deprecated in the current PAdES standards, and most modern validators
        don't rely on it being there.
        
        That said, there's no real harm in creating these entries, other than
        that it occasionally forces DSS updates where none would otherwise
        be necessary, and that it prevents the DSS from being updated prior
        to signing (as opposed to after signing).
    """

    skip_if_unneeded: bool = True
    """
    Do not perform a write if updating the DSS would not add any new
    information.
    
    .. note::
        This setting is only used if the DSS update would happen in its own
        revision.
    """


class SigDSSPlacementPreference(enum.Enum):
    """
    .. versionadded:: 0.8.0

    Preference for where to perform a DSS update with validation information
    for a specific signature.
    """

    TOGETHER_WITH_SIGNATURE = enum.auto()
    """
    Update the DSS in the revision that contains the signature.
    Doing so can be useful to create a PAdES-B-LT signature in a single
    revision.
    Such signatures can be processed by a validator that isn't capable of
    incremental update analysis.
    
    .. warning::
        This setting can only be used if :attr:`include_vri` is ``False``.
    """

    SEPARATE_REVISION = enum.auto()
    """
    Always perform the DSS update in a separate revision, after the signature,
    but before any timestamps are added.
    
    .. note::
        This is the old default behaviour.
    """

    TOGETHER_WITH_NEXT_TS = enum.auto()
    """
    If the signing workflow includes a document timestamp after the signature,
    update the DSS in the same revision as the timestamp.
    In the absence of document timestamps, this is equivalent to
    :attr:`SEPARATE_REVISION`.

    .. warning::
        This option controls the addition of validation info for the signature
        and its associated signature timestamp, not the validation info for the
        document timestamp itself.
        See :attr:`.DSSContentSettings.next_ts_settings`.

        In most practical situations, the distinction is only relevant in
        interrupted signing workflows (see :ref:`interrupted-signing`),
        where the lifecycle of the validation context is out of pyHanko's hands.
    """


@dataclass(frozen=True)
class TimestampDSSContentSettings(GeneralDSSContentSettings):
    """
    .. versionadded:: 0.8.0

    Settings for a DSS update with validation information for a document
    timestamp.

    .. note::
        In most workflows, adding a document timestamp doesn't trigger any DSS
        updates beyond VRI additions, because the same TSA is used for signature
        timestamps and for document timestamps.
    """

    update_before_ts: bool = False
    """
    Perform DSS update before creating the timestamp, instead of after.

    .. warning::
        This setting can only be used if :attr:`include_vri` is ``False``.
    """

    def assert_viable(self):
        """
        Check settings for consistency, and raise :class:`.SigningError`
        otherwise.
        """
        if self.include_vri and self.update_before_ts:
            raise SigningError(
                "If VRI entries are to be included, DSS updates can only be "
                "performed after the timestamp in question was created."
            )


@dataclass(frozen=True)
class DSSContentSettings(GeneralDSSContentSettings):
    """
    .. versionadded:: 0.8.0

    Settings for a DSS update with validation information for a signature.
    """

    placement: SigDSSPlacementPreference = (
        SigDSSPlacementPreference.TOGETHER_WITH_NEXT_TS
    )
    """
    Preference for where to perform a DSS update with validation information
    for a specific signature. See :class:`.SigDSSPlacementPreference`.
    
    The default is :attr:`.SigDSSPlacementPreference.TOGETHER_WITH_NEXT_TS`.
    """

    next_ts_settings: Optional[TimestampDSSContentSettings] = None
    """
    Explicit settings for DSS updates pertaining to a document timestamp
    added as part of the same signing workflow, if applicable.
    
    If ``None``, a default will be generated based on the values of this
    settings object.
    
    .. note::
        When consuming :class:`.DSSContentSettings` objects, you should
        call :meth:`get_settings_for_ts` instead of relying on the value of
        this field.
    """

    def get_settings_for_ts(self) -> TimestampDSSContentSettings:
        """
        Retrieve DSS update settings for document timestamps that are
        part of our signing workflow, if there are any.
        """
        ts_settings = self.next_ts_settings
        if ts_settings is not None:
            return ts_settings
        update_before_ts = (
            self.placement == SigDSSPlacementPreference.TOGETHER_WITH_SIGNATURE
        )
        return TimestampDSSContentSettings(
            include_vri=self.include_vri,
            skip_if_unneeded=self.skip_if_unneeded,
            update_before_ts=update_before_ts,
        )

    def assert_viable(self):
        """
        Check settings for consistency, and raise :class:`.SigningError`
        otherwise.
        """
        pre_sign = (
            self.placement == SigDSSPlacementPreference.TOGETHER_WITH_SIGNATURE
        )
        if self.include_vri and pre_sign:
            raise SigningError(
                "If VRI entries are to be included, DSS updates can only be "
                "performed after the signature in question was created."
            )
        self.get_settings_for_ts().assert_viable()


@dataclass(frozen=True)
class PdfSignatureMetadata:
    """
    Specification for a PDF signature.
    """

    field_name: Optional[str] = None
    """
    The name of the form field to contain the signature.
    If there is only one available signature field, the name may be inferred.
    """

    md_algorithm: Optional[str] = None
    """
    The name of the digest algorithm to use.
    It should be supported by `pyca/cryptography`.

    If ``None``, :func:`.select_suitable_signing_md` will be invoked to generate
    a suitable default, unless a seed value dictionary happens to be available.
    """

    location: Optional[str] = None
    """
    Location of signing.
    """

    reason: Optional[str] = None
    """
    Reason for signing (textual).
    """

    contact_info: Optional[str] = None
    """
    Information provided by the signer to enable the receiver to contact the
    signer to verify the signature.
    """

    name: Optional[str] = None
    """
    Name of the signer. This value is usually not necessary to set, since
    it should appear on the signer's certificate, but there are cases
    where it might be useful to specify it here (e.g. in situations where 
    signing is delegated to a trusted third party).
    """

    app_build_props: Optional[BuildProps] = None
    """
    Properties of the application that created the signature.
    
    If specified, this data will be recorded in the **Prop_Build**
    dictionary of the signature.
    """

    prop_auth_time: Optional[int] = None
    """
    Number of seconds since signer was last authenticated.
    """

    prop_auth_type: Optional[SigAuthType] = None
    """
    Signature /Prop_AuthType to use.

    This should be one of 
    :attr:`~.fields.SigAuthType.PIN` or
    :attr:`~.fields.SigAuthType.PASSWORD` or
    :attr:`~.fields.SigAuthType.FINGERPRINT`
    If not specified, this property won't be set on the signature dictionary.
    """

    certify: bool = False
    """
    Sign with an author (certification) signature, as opposed to an approval
    signature. A document can contain at most one such signature, and it must
    be the first one.
    """
    # TODO Does this restriction also apply to prior document timestamps?

    subfilter: Optional[SigSeedSubFilter] = None
    """
    Signature subfilter to use.

    This should be one of 
    :attr:`~.fields.SigSeedSubFilter.ADOBE_PKCS7_DETACHED` or
    :attr:`~.fields.SigSeedSubFilter.PADES`.
    If not specified, the value may be inferred from the signature field's
    seed value dictionary. Failing that,
    :attr:`~.fields.SigSeedSubFilter.ADOBE_PKCS7_DETACHED` is used as the
    default value.
    """

    embed_validation_info: bool = False
    """
    Flag indicating whether validation info (OCSP responses and/or CRLs)
    should be embedded or not. This is necessary to be able to validate
    signatures long after they have been made.
    This flag requires :attr:`validation_context` to be set.

    The precise manner in which the validation info is embedded depends on
    the (effective) value of :attr:`subfilter`:

    * With :attr:`~.fields.SigSeedSubFilter.ADOBE_PKCS7_DETACHED`, the
      validation information will be embedded inside the CMS object containing
      the signature.
    * With :attr:`~.fields.SigSeedSubFilter.PADES`, the validation information
      will be embedded into the document security store (DSS).
    """

    use_pades_lta: bool = False
    """
    If ``True``, the signer will append an additional document timestamp after
    writing the signature's validation information to the document security
    store (DSS).
    This flag is only meaningful if :attr:`subfilter` is 
    :attr:`~.fields.SigSeedSubFilter.PADES`.

    The PAdES B-LTA profile solves the long-term validation problem by
    adding a timestamp chain to the document after the regular signatures, which
    is updated with new timestamps at regular intervals.
    This provides an audit trail that ensures the long-term integrity of the 
    validation information in the DSS, since OCSP responses and CRLs also have 
    a finite lifetime.

    See also :meth:`.PdfTimeStamper.update_archival_timestamp_chain`.
    """

    timestamp_field_name: Optional[str] = None
    """
    Name of the timestamp field created when :attr:`use_pades_lta` is ``True``.
    If not specified, a unique name will be generated using :mod:`uuid`.
    """

    validation_context: Optional[ValidationContext] = None
    """
    The validation context to use when validating signatures.
    If provided, the signer's certificate and any timestamp certificates
    will be validated before signing.

    This parameter is mandatory when :attr:`embed_validation_info` is ``True``.
    """

    docmdp_permissions: MDPPerm = MDPPerm.FILL_FORMS
    """
    Indicates the document modification policy that will be in force after    
    this signature is created. Only relevant for certification signatures
    or signatures that apply locking.

    .. warning::
        For non-certification signatures, this is only explicitly allowed since 
        PDF 2.0 (ISO 32000-2), so older software may not respect this setting
        on approval signatures.
    """

    signer_key_usage: Set[str] = field(
        default_factory=lambda: constants.DEFAULT_SIGNER_KEY_USAGE
    )
    """
    Key usage extensions required for the signer's certificate.
    Defaults to ``non_repudiation`` only, but sometimes ``digital_signature``
    or a combination of both may be more appropriate.
    See :class:`x509.KeyUsage` for a complete list.

    Only relevant if a validation context is also provided.
    """

    cades_signed_attr_spec: Optional[CAdESSignedAttrSpec] = None
    """
    .. versionadded:: 0.5.0

    Specification for CAdES-specific attributes.
    """

    dss_settings: DSSContentSettings = DSSContentSettings()
    """
    .. versionadded:: 0.8.0

    DSS output settings. See :class:`.DSSContentSettings`.
    """

    tight_size_estimates: bool = False
    """
    .. versionadded:: 0.8.0

    When estimating the size of a signature container,
    do not add safety margins.
    
    .. note::
        This should be OK if the entire CMS object is produced by pyHanko, and
        the signing scheme produces signatures of a fixed size.
        However, if the signature container includes unsigned attributes such
        as signature timestamps, the size of the signature is never entirely
        predictable.
    """

    ac_validation_context: Optional[ValidationContext] = None
    """
    .. versionadded:: 0.11.0

    Validation context for attribute certificates
    """


def _ensure_esic_ext(pdf_writer: BasePdfFileWriter):
    """
    Helper function to ensure that the output PDF is at least PDF 1.7, and that
    the relevant ESIC extension for PAdES is enabled if the version lower than
    2.0.
    """
    pdf_writer.ensure_output_version(version=(1, 7))
    if pdf_writer.output_version < (2, 0):
        pdf_writer.register_extension(constants.ESIC_EXTENSION_1)


def add_mac_to_external_cms(
    pdf_out: BasePdfFileWriter,
    md_algorithm: str,
    signed_data: cms.SignedData,
    document_digest: bytes,
    dry_run=False,
):
    """
    Utility function to add a MAC token to an externally generated CMS
    payload. The payload is modified in-place.

    :param pdf_out:
        Writer to contain output.
    :param md_algorithm:
        The message digest algorithm to use for the MAC.
    :param document_digest:
        The document digest that should go into the MAC token.
    :param signed_data:
        A CMS signed data value.
    :param dry_run:
        Whether this is considered a dry run for size estimation purposes.
    """

    mac_handler = pdf_out._init_mac_handler(md_algorithm)
    si = signed_data['signer_infos'][0]
    signature_bytes = si['signature'].native
    md = hashes.Hash(get_pyca_cryptography_hash(md_algorithm))
    md.update(signature_bytes)
    sig_digest = md.finalize()
    if dry_run:
        # sig_digest always has the right length
        document_digest = sig_digest
    mac_token = mac_handler.build_pdfmac_token(
        document_digest=document_digest,
        signature_digest=sig_digest,
        dry_run=dry_run,
    )
    ua = si['unsigned_attrs']
    if ua is VOID:
        ua = cms.CMSAttributes([])
    ua.append(simple_cms_attribute('pdf_mac_data', mac_token))
    si['unsigned_attrs'] = ua


def _ensure_iso32001_ext(pdf_writer: BasePdfFileWriter):
    pdf_writer.ensure_output_version(version=(2, 0))
    pdf_writer.register_extension(constants.ISO32001)


def _ensure_iso32002_ext(pdf_writer: BasePdfFileWriter):
    pdf_writer.ensure_output_version(version=(2, 0))
    pdf_writer.register_extension(constants.ISO32002)


def _is_iso32002_curve(pubkey: keys.PublicKeyInfo):
    kind, curve_id = pubkey.curve
    return kind == 'named' and curve_id in constants.ISO32002_CURVE_NAMES


class PdfTimeStamper:
    """
    Class to encapsulate the process of appending document timestamps to
    PDF files.
    """

    def __init__(
        self,
        timestamper: TimeStamper,
        field_name: Optional[str] = None,
        invis_settings: InvisSigSettings = InvisSigSettings(),
        readable_field_name: str = "Timestamp",
    ):
        self.default_timestamper = timestamper
        self._field_name = field_name
        self._readable_field_name = readable_field_name
        self._invis_settings = invis_settings

    @property
    def field_name(self) -> str:
        """
        Retrieve or generate the field name for the signature field to contain
        the document timestamp.

        :return:
            The field name, as a (Python) string.
        """
        return self._field_name or ('Timestamp-' + str(uuid.uuid4()))

    # TODO I'm not entirely sure that allowing validation_paths to be cached
    #  is wise. In principle, the TSA could issue their next timestamp with a
    #  different certificate (e.g. due to load balancing), which would require
    #  validation regardless.

    def timestamp_pdf(
        self,
        pdf_out: IncrementalPdfFileWriter,
        md_algorithm,
        validation_context=None,
        bytes_reserved=None,
        validation_paths=None,
        timestamper: Optional[TimeStamper] = None,
        *,
        in_place=False,
        output=None,
        dss_settings: TimestampDSSContentSettings = TimestampDSSContentSettings(),
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
        tight_size_estimates: bool = False,
    ):
        """
        .. versionchanged:: 0.9.0
            Wrapper around :meth:`async_timestamp_pdf`.

        Timestamp the contents of ``pdf_out``.
        Note that ``pdf_out`` should not be written to after this operation.

        :param pdf_out:
            An :class:`.IncrementalPdfFileWriter`.
        :param md_algorithm:
            The hash algorithm to use when computing message digests.
        :param validation_context:
            The :class:`.pyhanko_certvalidator.ValidationContext`
            against which the TSA response should be validated.
            This validation context will also be used to update the DSS.
        :param bytes_reserved:
            Bytes to reserve for the CMS object in the PDF file.
            If not specified, make an estimate based on a dummy signature.

            .. warning::
                Since the CMS object is written to the output file as a
                hexadecimal string, you should request **twice** the (estimated)
                number of bytes in the DER-encoded version of the CMS object.
        :param validation_paths:
            If the validation path(s) for the TSA's certificate are already
            known, you can pass them using this parameter to avoid having to
            run the validation logic again.
        :param timestamper:
            Override the default :class:`.TimeStamper` associated with this
            :class:`.PdfTimeStamper`.
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :param dss_settings:
            DSS output settings. See :class:`.TimestampDSSContentSettings`.
        :param tight_size_estimates:
            When estimating the size of a document timestamp container,
            do not add safety margins.

            .. note::
                External TSAs cannot be relied upon to always produce the
                exact same output length, which makes this option risky to use.
        :return:
            The output stream containing the signed output.
        """
        result = asyncio.run(
            self.async_timestamp_pdf(
                pdf_out,
                md_algorithm,
                validation_context=validation_context,
                bytes_reserved=bytes_reserved,
                validation_paths=validation_paths,
                timestamper=timestamper,
                in_place=in_place,
                output=output,
                chunk_size=chunk_size,
                dss_settings=dss_settings,
                tight_size_estimates=tight_size_estimates,
            )
        )
        return result

    async def async_timestamp_pdf(
        self,
        pdf_out: IncrementalPdfFileWriter,
        md_algorithm,
        validation_context=None,
        bytes_reserved=None,
        validation_paths=None,
        timestamper: Optional[TimeStamper] = None,
        *,
        in_place=False,
        output=None,
        dss_settings: TimestampDSSContentSettings = TimestampDSSContentSettings(),
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
        tight_size_estimates: bool = False,
        embed_roots: bool = True,
    ):
        """
        .. versionadded:: 0.9.0

        Timestamp the contents of ``pdf_out``.
        Note that ``pdf_out`` should not be written to after this operation.

        :param pdf_out:
            An :class:`.IncrementalPdfFileWriter`.
        :param md_algorithm:
            The hash algorithm to use when computing message digests.
        :param validation_context:
            The :class:`.pyhanko_certvalidator.ValidationContext`
            against which the TSA response should be validated.
            This validation context will also be used to update the DSS.
        :param bytes_reserved:
            Bytes to reserve for the CMS object in the PDF file.
            If not specified, make an estimate based on a dummy signature.

            .. warning::
                Since the CMS object is written to the output file as a
                hexadecimal string, you should request **twice** the (estimated)
                number of bytes in the DER-encoded version of the CMS object.
        :param validation_paths:
            If the validation path(s) for the TSA's certificate are already
            known, you can pass them using this parameter to avoid having to
            run the validation logic again.
        :param timestamper:
            Override the default :class:`.TimeStamper` associated with this
            :class:`.PdfTimeStamper`.
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :param dss_settings:
            DSS output settings. See :class:`.TimestampDSSContentSettings`.
        :param tight_size_estimates:
            When estimating the size of a document timestamp container,
            do not add safety margins.

            .. note::
                External TSAs cannot be relied upon to always produce the
                exact same output length, which makes this option risky to use.
        :param embed_roots:
            Option that controls whether the root certificate of each validation
            path should be embedded into the DSS. The default is ``True``.

            .. note::
                Trust roots are configured by the validator, so embedding them
                typically does nothing in a typical validation process.
                Therefore they can be safely omitted in most cases.
                Nonetheless, embedding the roots can be useful for documentation
                purposes.
        :return:
            The output stream containing the signed output.
        """

        _ensure_esic_ext(pdf_out)
        need_mac = (
            pdf_out.security_handler is not None
            and pdf_out.security_handler.pdf_mac_enabled
        )
        from pyhanko.sign import validation

        timestamper = timestamper or self.default_timestamper
        if validation_context is not None:
            paths_coro = timestamper.validation_paths(validation_context)
            if validation_paths is None:
                validation_paths = []
            async for path in paths_coro:
                validation_paths.append(path)
            dss_settings.assert_viable()
            if dss_settings.update_before_ts:
                # NOTE: we have to disable VRI in this scenario
                validation.DocumentSecurityStore.supply_dss_in_writer(
                    pdf_out,
                    sig_contents=None,
                    paths=validation_paths,
                    validation_context=validation_context,
                    embed_roots=embed_roots,
                )

        field_name = self.field_name
        if bytes_reserved is None:
            test_tst = await timestamper.async_dummy_response(md_algorithm)
            if need_mac:
                add_mac_to_external_cms(
                    pdf_out,
                    md_algorithm=md_algorithm,
                    signed_data=test_tst['content'],
                    document_digest=b"",
                    dry_run=True,
                )
            test_len = len(test_tst.dump()) * 2
            if tight_size_estimates:
                bytes_reserved = test_len
            else:
                # see sign_pdf comments
                bytes_reserved = test_len + 2 * (test_len // 4)

        timestamp_obj = DocumentTimestamp(bytes_reserved=bytes_reserved)

        field_spec = SigFieldSpec(
            sig_field_name=field_name,
            invis_sig_settings=self._invis_settings,
            readable_field_name=self._readable_field_name,
        )
        cms_writer = PdfCMSEmbedder(new_field_spec=field_spec).write_cms(
            field_name=field_name,
            writer=pdf_out,
            # for LTA, requiring existing_fields_only doesn't make sense
            # since we should in principle be able to add document timestamps
            # ad infinitum.
            existing_fields_only=False,
        )

        next(cms_writer)
        sig_obj_ref = cms_writer.send(
            SigObjSetup(sig_placeholder=timestamp_obj)
        )
        if need_mac:
            _register_mac_on_sig(pdf_out, sig_obj_ref)

        sig_io = SigIOSetup(
            md_algorithm=md_algorithm,
            in_place=in_place,
            output=output,
            chunk_size=chunk_size,
        )
        prep_digest: PreparedByteRangeDigest
        prep_digest, res_output = cms_writer.send(sig_io)
        timestamp_cms = await timestamper.async_timestamp(
            prep_digest.document_digest, md_algorithm
        )
        if need_mac:
            add_mac_to_external_cms(
                pdf_out,
                md_algorithm=md_algorithm,
                signed_data=timestamp_cms['content'],
                document_digest=prep_digest.document_digest,
            )
        sig_contents = cms_writer.send(timestamp_cms)

        # update the DSS if necessary
        if validation_context is not None and not dss_settings.update_before_ts:
            if not dss_settings.include_vri:
                sig_contents = None

            # FIXME in this case, the ser/deser step is unnecessary
            #  and inefficient; should probably rewrite
            #  using supply_dss_in_writer
            credential_ser: Optional[SerialisedCredential] = None
            if pdf_out.security_handler:
                credential = pdf_out.security_handler.extract_credential()
                if credential:
                    credential_ser = credential.serialise()
            validation.DocumentSecurityStore.add_dss(
                output_stream=res_output,
                sig_contents=sig_contents,
                paths=validation_paths,
                validation_context=validation_context,
                force_write=not dss_settings.skip_if_unneeded,
                embed_roots=embed_roots,
                file_credential=credential_ser,
            )

        return misc.finalise_output(output, res_output)

    def update_archival_timestamp_chain(
        self,
        reader: PdfFileReader,
        validation_context,
        in_place=True,
        output=None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
        default_md_algorithm=constants.DEFAULT_MD,
    ):
        """
        .. versionchanged:: 0.9.0
            Wrapper around :meth:`async_update_archival_timestamp_chain`.

        Validate the last timestamp in the timestamp chain on a PDF file, and
        write an updated version to an output stream.

        :param reader:
            A :class:`PdfReader` encapsulating the input file.
        :param validation_context:
            :class:`.pyhanko_certvalidator.ValidationContext` object to validate
            the last timestamp.
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :param default_md_algorithm:
            Message digest to use if there are no preceding timestamps in the
            file.
        :return:
            The output stream containing the signed output.
        """
        coro = self.async_update_archival_timestamp_chain(
            reader=reader,
            validation_context=validation_context,
            in_place=in_place,
            output=output,
            chunk_size=chunk_size,
            default_md_algorithm=default_md_algorithm,
        )
        return asyncio.run(coro)

    async def async_update_archival_timestamp_chain(
        self,
        reader: PdfFileReader,
        validation_context,
        in_place=True,
        output=None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
        default_md_algorithm=constants.DEFAULT_MD,
        embed_roots: bool = True,
    ):
        """
        .. versionadded:: 0.9.0

        Validate the last timestamp in the timestamp chain on a PDF file, and
        write an updated version to an output stream.

        :param reader:
            A :class:`PdfReader` encapsulating the input file.
        :param validation_context:
            :class:`.pyhanko_certvalidator.ValidationContext` object to validate
            the last timestamp.
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :param default_md_algorithm:
            Message digest to use if there are no preceding timestamps in the
            file.
        :param embed_roots:
            Option that controls whether the root certificate of each validation
            path should be embedded into the DSS. The default is ``True``.

            .. note::
                Trust roots are configured by the validator, so embedding them
                typically does nothing in a typical validation process.
                Therefore they can be safely omitted in most cases.
                Nonetheless, embedding the roots can be useful for documentation
                purposes.
        :return:
            The output stream containing the signed output.
        """

        # TODO expose DSS fine-tuning here as well

        # In principle, we only have to validate that the last timestamp token
        # in the current chain is valid.
        # TODO: add an option to validate the entire timestamp chain
        #  plus all signatures
        from .. import validation
        from ..validation.status import TimestampSignatureStatus

        timestamps = validation.get_timestamp_chain(reader)
        try:
            last_timestamp = next(timestamps)
        except StopIteration:
            logger.warning(
                "Document does not have any document timestamps yet. "
                "This may cause unexpected results."
            )
            last_timestamp = None

        # Validate the previous timestamp if present
        tst_status: Optional[TimestampSignatureStatus] = None
        if last_timestamp is None:
            md_algorithm = default_md_algorithm
        else:
            expected_imprint = last_timestamp.compute_digest()

            tst_token = last_timestamp.signed_data

            # run validation logic
            last_tst_status = await validation.ltv.establish_timestamp_trust(
                tst_token, validation_context, expected_imprint
            )

            md_algorithm = last_tst_status.md_algorithm
            tst_status = last_tst_status

        # Prepare output
        if in_place:
            res_output = reader.stream
        else:
            res_output = misc.prepare_rw_output_stream(output)
            reader.stream.seek(0)
            misc.chunked_write(bytearray(chunk_size), reader.stream, res_output)

        pdf_out = IncrementalPdfFileWriter(res_output)
        if last_timestamp is not None:
            assert tst_status is not None
            # update the DSS
            validation.DocumentSecurityStore.supply_dss_in_writer(
                pdf_out,
                last_timestamp.pkcs7_content,
                paths=(tst_status.validation_path,),
                validation_context=validation_context,
                embed_roots=embed_roots,
            )

        # append a new timestamp
        await self.async_timestamp_pdf(
            pdf_out,
            md_algorithm,
            validation_context,
            in_place=True,
            embed_roots=embed_roots,
        )
        return misc.finalise_output(output, res_output)


def _signatures_exist(handler):
    try:
        next(enumerate_sig_fields(handler, filled_status=True))
        return True
    except StopIteration:
        return False


class PdfSigner:
    """
    .. versionchanged: 0.7.0
        This class is no longer a subclass of :class:`.PdfTimeStamper`.

    Class to handle PDF signatures in general.

    :param signature_meta:
        The specification of the signature to add.
    :param signer:
        :class:`.Signer` object to use to produce the signature object.
    :param timestamper:
        :class:`.TimeStamper` object to use to produce any time stamp tokens
        that might be required.
    :param stamp_style:
        Stamp style specification to determine the visible style of the
        signature, typically an object of type :class:`.TextStampStyle` or
        :class:`.QRStampStyle`. Defaults to
        :const:`constants.DEFAULT_SIGNING_STAMP_STYLE`.
    :param new_field_spec:
        If a new field is to be created, this parameter allows the caller
        to specify the field's properties in the form of a
        :class:`.SigFieldSpec`. This parameter is only meaningful if
        ``existing_fields_only`` is ``False``.
    """

    _ignore_sv = False

    def __init__(
        self,
        signature_meta: PdfSignatureMetadata,
        signer: Signer,
        *,
        timestamper: Optional[TimeStamper] = None,
        stamp_style: Optional[BaseStampStyle] = None,
        new_field_spec: Optional[SigFieldSpec] = None,
    ):
        self.signature_meta = signature_meta
        if (
            new_field_spec is not None
            and new_field_spec.sig_field_name != signature_meta.field_name
        ):
            raise SigningError(
                "Field names specified in SigFieldSpec and "
                "PdfSignatureMetadata do not agree."
            )

        self.signer = signer
        stamp_style = stamp_style or constants.DEFAULT_SIGNING_STAMP_STYLE
        self.stamp_style: BaseStampStyle = stamp_style

        self.signer_hash_algo: Optional[str]
        try:
            mech = self.signer.get_signature_mechanism_for_digest(None)
            self.signer_hash_algo = get_cms_hash_algo_for_mechanism(mech)
        except ValueError:
            self.signer_hash_algo = None

        self.new_field_spec = new_field_spec
        self.default_timestamper = timestamper

    @property
    def default_md_for_signer(self) -> Optional[str]:
        """
        Name of the default message digest algorithm for this signer, if there
        is one.
        This method will try the :attr:`~.PdfSignatureMetadata.md_algorithm`
        attribute on the signer's :attr:`signature_meta`, or try to retrieve
        the digest algorithm associated with the underlying
        :class:`~pyhanko.sign.signers.pdf_cms.Signer`.

        :return:
            The name of the message digest algorithm, or ``None``.
        """
        return self.signature_meta.md_algorithm or self.signer_hash_algo

    def _enforce_certification_constraints(self, reader: PdfFileReader):
        # TODO we really should take into account the /DocMDP constraints
        #  of _all_ previous signatures, i.e. also approval signatures with
        #  locking instructions etc.
        if self.signature_meta.certify and _signatures_exist(reader):
            raise SigningError(
                "Certification signatures must be the first signature "
                "in a given document."
            )

        from pyhanko.sign.validation import read_certification_data

        cd = read_certification_data(reader)
        # if there is no author signature, we don't have to do anything
        if cd is None:
            return
        if cd.permission == MDPPerm.NO_CHANGES:
            raise SigningError("Author signature forbids all changes")

    def _retrieve_seed_value_spec(
        self, sig_field
    ) -> Optional[SigSeedValueSpec]:
        # for testing & debugging
        if self._ignore_sv:
            return None
        sv_dict = sig_field.get('/SV')
        if sv_dict is None:
            return None
        return SigSeedValueSpec.from_pdf_object(sv_dict)

    def _select_md_algorithm(self, sv_spec: Optional[SigSeedValueSpec]) -> str:
        signature_meta = self.signature_meta

        # priority order for the message digest algorithm
        #  (1) If signature_meta specifies a message digest algorithm, use it
        #      (it has been cleared by the SV dictionary checker already)
        #  (2) Use the first algorithm specified in the seed value dictionary,
        #      if a suggestion is present
        #  (3) fall back to select_suitable_signing_md()
        if sv_spec is not None and sv_spec.digest_methods:
            sv_md_algorithm = sv_spec.digest_methods[0]
        else:
            sv_md_algorithm = None

        if self.default_md_for_signer is not None:
            md_algorithm = self.default_md_for_signer
        elif sv_md_algorithm is not None:
            md_algorithm = sv_md_algorithm
        elif self.signer.signing_cert is not None:
            md_algorithm = select_suitable_signing_md(
                self.signer.signing_cert.public_key
            )
        else:
            raise SigningError(
                "Could not select a default digest algorithm. Please supply "
                "a value in the signature settings, or configure the signer "
                "with an explicit signature mechanism that includes a digest "
                "algorithm specification."
            )

        algorithm_policy = (
            signature_meta.validation_context.algorithm_policy
            if signature_meta.validation_context is not None
            else None
        )
        if algorithm_policy is not None:
            now = datetime.now()
            md_algo_obj = algos.DigestAlgorithm({'algorithm': md_algorithm})
            if not algorithm_policy.digest_algorithm_allowed(md_algo_obj, now):
                raise SigningError(
                    f"The hash algorithm {md_algorithm} is not allowed in the "
                    f"specified validation context (usage policy of type "
                    f"{algorithm_policy.__class__.__name__})."
                )
        return md_algorithm

    def register_extensions(
        self, pdf_out: BasePdfFileWriter, *, md_algorithm: str
    ):
        if self.signature_meta.subfilter == SigSeedSubFilter.PADES:
            _ensure_esic_ext(pdf_out)

        if (
            pdf_out.security_handler is not None
            and pdf_out.security_handler.pdf_mac_enabled
        ):
            pdf_out.register_extension(pdfmac.ISO32004)

        try:
            sig_mech = self.signer.get_signature_mechanism_for_digest(
                md_algorithm
            )
            sig_algo = sig_mech.signature_algo
        except (SigningError, ValueError) as e:
            logger.debug(
                f"Failed to introspect signature mechanism: {str(e)}. "
                f"Will forgo algorithm-based automatic extension registration.",
            )
            return
        if sig_algo == 'ed25519':
            _ensure_iso32002_ext(pdf_out)
        elif sig_algo == 'ed448':
            _ensure_iso32001_ext(pdf_out)
            _ensure_iso32002_ext(pdf_out)
        else:
            if md_algorithm.startswith('sha3') or md_algorithm == 'shake256':
                _ensure_iso32001_ext(pdf_out)
            cert = self.signer.signing_cert
            if (
                cert
                and sig_algo == 'ecdsa'
                and _is_iso32002_curve(cert.public_key)
            ):
                _ensure_iso32002_ext(pdf_out)

    def init_signing_session(
        self, pdf_out: BasePdfFileWriter, existing_fields_only=False
    ) -> 'PdfSigningSession':
        """
        Initialise a signing session with this :class:`.PdfSigner` for a
        specified PDF file writer.

        This step in the signing process handles all field-level operations
        prior to signing: it creates the target form field if necessary, and
        makes sure the seed value dictionary gets processed.

        See also :meth:`digest_doc_for_signing` and :meth:`sign_pdf`.

        :param pdf_out:
            The writer containing the PDF file to be signed.
        :param existing_fields_only:
            If ``True``, never create a new empty signature field to contain
            the signature.
            If ``False``, a new field may be created if no field matching
            :attr:`~.PdfSignatureMetadata.field_name` exists.
        :return:
            A :class:`.PdfSigningSession` object modelling the signing session
            in its post-setup stage.
        """

        if isinstance(pdf_out, IncrementalPdfFileWriter):
            # ensure we're not signing a hybrid reference doc
            prev = pdf_out.prev
            if prev.strict and prev.xrefs.hybrid_xrefs_present:
                raise SigningError(
                    "Attempting to sign document with hybrid cross-reference "
                    "sections while hybrid xrefs are disabled"
                )

        timestamper = self.default_timestamper

        signature_meta: PdfSignatureMetadata = self.signature_meta

        cms_writer = PdfCMSEmbedder(
            new_field_spec=self.new_field_spec
        ).write_cms(
            field_name=signature_meta.field_name,
            writer=pdf_out,
            existing_fields_only=existing_fields_only,
        )

        # let the CMS writer put in a field for us, if necessary
        sig_field_ref = next(cms_writer)

        sig_field = sig_field_ref.get_object()

        # Fetch seed values (if present) to prepare for signing
        sv_spec = self._retrieve_seed_value_spec(sig_field)

        # Check DocMDP settings to see if we're allowed to add a signature
        if isinstance(pdf_out, IncrementalPdfFileWriter):
            self._enforce_certification_constraints(pdf_out.prev)

        md_algorithm = self._select_md_algorithm(sv_spec)
        self.register_extensions(pdf_out, md_algorithm=md_algorithm)

        # check if a timestamp is required without a timestamper being
        # available already, and furnish one if necessary
        if (
            sv_spec is not None
            and sv_spec.timestamp_required
            and timestamper is None
        ):
            timestamper = sv_spec.build_timestamper()

        # subfilter: try signature_meta and SV dict, fall back
        #  to /adbe.pkcs7.detached by default
        subfilter = signature_meta.subfilter
        if subfilter is None:
            if sv_spec is not None and sv_spec.subfilters:
                subfilter = sv_spec.subfilters[0]
            else:
                subfilter = SigSeedSubFilter.ADOBE_PKCS7_DETACHED

        session = PdfSigningSession(
            self,
            pdf_out,
            cms_writer,
            sig_field,
            md_algorithm,
            timestamper,
            subfilter,
            sv_spec=sv_spec,
        )

        return session

    def digest_doc_for_signing(
        self,
        pdf_out: BasePdfFileWriter,
        existing_fields_only=False,
        bytes_reserved=None,
        *,
        appearance_text_params=None,
        in_place=False,
        output=None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
    ) -> Tuple[PreparedByteRangeDigest, 'PdfTBSDocument', IO]:
        """
        .. deprecated:: 0.9.0
            Use :meth:`async_digest_doc_for_signing` instead.

        Set up all stages of the signing process up to and including the point
        where the signature placeholder is allocated, and the document's
        ``/ByteRange`` digest is computed.

        See :meth:`sign_pdf` for a less granular, more high-level approach.

        .. note::
            This method is useful in remote signing scenarios, where you might
            want to free up resources while waiting for the remote signer to
            respond. The :class:`.PreparedByteRangeDigest` object returned
            allows you to keep track of the required state to fill the
            signature container at some later point in time.

        :param pdf_out:
            A PDF file writer (usually an :class:`.IncrementalPdfFileWriter`)
            containing the data to sign.
        :param existing_fields_only:
            If ``True``, never create a new empty signature field to contain
            the signature.
            If ``False``, a new field may be created if no field matching
            :attr:`~.PdfSignatureMetadata.field_name` exists.
        :param bytes_reserved:
            Bytes to reserve for the CMS object in the PDF file.
            If not specified, make an estimate based on a dummy signature.

            .. warning::
                Since the CMS object is written to the output file as a
                hexadecimal string, you should request **twice** the (estimated)
                number of bytes in the DER-encoded version of the CMS object.
        :param appearance_text_params:
            Dictionary with text parameters that will be passed to the
            signature appearance constructor (if applicable).
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :return:
            A tuple containing a :class:`.PreparedByteRangeDigest` object,
            a :class:`.PdfTBSDocument` object and an output handle to which the
            document in its current state has been written.
        """
        warnings.warn(
            "'digest_doc_for_signing' is deprecated, use "
            "'async_digest_doc_for_signing' instead",
            DeprecationWarning,
        )
        result = asyncio.run(
            self.async_digest_doc_for_signing(
                pdf_out,
                existing_fields_only=existing_fields_only,
                bytes_reserved=bytes_reserved,
                appearance_text_params=appearance_text_params,
                in_place=in_place,
                output=output,
                chunk_size=chunk_size,
            )
        )
        return result

    async def async_digest_doc_for_signing(
        self,
        pdf_out: BasePdfFileWriter,
        existing_fields_only=False,
        bytes_reserved=None,
        *,
        appearance_text_params=None,
        in_place=False,
        output=None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
    ) -> Tuple[PreparedByteRangeDigest, 'PdfTBSDocument', IO]:
        """
        .. versionadded:: 0.9.0

        Set up all stages of the signing process up to and including the point
        where the signature placeholder is allocated, and the document's
        ``/ByteRange`` digest is computed.

        See :meth:`sign_pdf` for a less granular, more high-level approach.

        .. note::
            This method is useful in remote signing scenarios, where you might
            want to free up resources while waiting for the remote signer to
            respond. The :class:`.PreparedByteRangeDigest` object returned
            allows you to keep track of the required state to fill the
            signature container at some later point in time.

        :param pdf_out:
            A PDF file writer (usually an :class:`.IncrementalPdfFileWriter`)
            containing the data to sign.
        :param existing_fields_only:
            If ``True``, never create a new empty signature field to contain
            the signature.
            If ``False``, a new field may be created if no field matching
            :attr:`~.PdfSignatureMetadata.field_name` exists.
        :param bytes_reserved:
            Bytes to reserve for the CMS object in the PDF file.
            If not specified, make an estimate based on a dummy signature.

            .. warning::
                Since the CMS object is written to the output file as a
                hexadecimal string, you should request **twice** the (estimated)
                number of bytes in the DER-encoded version of the CMS object.
        :param appearance_text_params:
            Dictionary with text parameters that will be passed to the
            signature appearance constructor (if applicable).
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :return:
            A tuple containing a :class:`.PreparedByteRangeDigest` object,
            a :class:`.PdfTBSDocument` object and an output handle to which the
            document in its current state has been written.
        """
        signing_session = self.init_signing_session(
            pdf_out,
            existing_fields_only=existing_fields_only,
        )
        validation_info = await signing_session.perform_presign_validation(
            pdf_out
        )
        if bytes_reserved is None:
            estimation = signing_session.estimate_signature_container_size(
                validation_info=validation_info,
                tight=self.signature_meta.tight_size_estimates,
            )
            bytes_reserved = await estimation

        tbs_document = signing_session.prepare_tbs_document(
            validation_info=validation_info,
            bytes_reserved=bytes_reserved,
            appearance_text_params=appearance_text_params,
        )
        prepared_br_digest, res_output = tbs_document.digest_tbs_document(
            in_place=in_place, chunk_size=chunk_size, output=output
        )
        return (
            prepared_br_digest,
            tbs_document,
            misc.finalise_output(output, res_output),
        )

    def sign_pdf(
        self,
        pdf_out: BasePdfFileWriter,
        existing_fields_only=False,
        bytes_reserved=None,
        *,
        appearance_text_params=None,
        in_place=False,
        output=None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
    ):
        """
        .. versionchanged:: 0.9.0
            Wrapper around :meth:`async_sign_pdf`.

        Sign a PDF file using the provided output writer.

        :param pdf_out:
            A PDF file writer (usually an :class:`.IncrementalPdfFileWriter`)
            containing the data to sign.
        :param existing_fields_only:
            If ``True``, never create a new empty signature field to contain
            the signature.
            If ``False``, a new field may be created if no field matching
            :attr:`~.PdfSignatureMetadata.field_name` exists.
        :param bytes_reserved:
            Bytes to reserve for the CMS object in the PDF file.
            If not specified, make an estimate based on a dummy signature.
        :param appearance_text_params:
            Dictionary with text parameters that will be passed to the
            signature appearance constructor (if applicable).
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :return:
            The output stream containing the signed data.
        """
        result = asyncio.run(
            self.async_sign_pdf(
                pdf_out,
                existing_fields_only=existing_fields_only,
                bytes_reserved=bytes_reserved,
                appearance_text_params=appearance_text_params,
                in_place=in_place,
                output=output,
                chunk_size=chunk_size,
            )
        )
        return result

    async def async_sign_pdf(
        self,
        pdf_out: BasePdfFileWriter,
        existing_fields_only=False,
        bytes_reserved=None,
        *,
        appearance_text_params=None,
        in_place=False,
        output=None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
    ):
        """
        .. versionadded:: 0.9.0

        Sign a PDF file using the provided output writer.

        :param pdf_out:
            A PDF file writer (usually an :class:`.IncrementalPdfFileWriter`)
            containing the data to sign.
        :param existing_fields_only:
            If ``True``, never create a new empty signature field to contain
            the signature.
            If ``False``, a new field may be created if no field matching
            :attr:`~.PdfSignatureMetadata.field_name` exists.
        :param bytes_reserved:
            Bytes to reserve for the CMS object in the PDF file.
            If not specified, make an estimate based on a dummy signature.
        :param appearance_text_params:
            Dictionary with text parameters that will be passed to the
            signature appearance constructor (if applicable).
        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :return:
            The output stream containing the signed data.
        """

        signing_session = self.init_signing_session(
            pdf_out,
            existing_fields_only=existing_fields_only,
        )
        validation_info = await signing_session.perform_presign_validation(
            pdf_out
        )
        if bytes_reserved is None:
            estimation = signing_session.estimate_signature_container_size(
                validation_info, tight=self.signature_meta.tight_size_estimates
            )
            bytes_reserved = await estimation
        tbs_document = signing_session.prepare_tbs_document(
            validation_info=validation_info,
            bytes_reserved=bytes_reserved,
            appearance_text_params=appearance_text_params,
        )
        prepared_br_digest, res_output = tbs_document.digest_tbs_document(
            in_place=in_place, chunk_size=chunk_size, output=output
        )

        post_signing_doc = await tbs_document.perform_signature(
            document_digest=prepared_br_digest.document_digest,
            pdf_cms_signed_attrs=PdfCMSSignedAttributes(
                signing_time=signing_session.system_time,
                adobe_revinfo_attr=(
                    None
                    if validation_info is None
                    else validation_info.adobe_revinfo_attr
                ),
                cades_signed_attrs=self.signature_meta.cades_signed_attr_spec,
            ),
        )

        await post_signing_doc.post_signature_processing(
            res_output, chunk_size=chunk_size
        )
        # we put the finalisation step after the DSS manipulations, since
        # otherwise we'd also run into issues with non-seekable output buffers
        return misc.finalise_output(output, res_output)


@dataclass(frozen=True)
class PreSignValidationStatus:
    """
    .. versionadded:: 0.7.0

    Container for validation data collected prior to creating a signature, e.g.
    for later inclusion in a document's DSS, or as a signed attribute on
    the signature.
    """

    signer_path: ValidationPath
    """
    Validation path for the signer's certificate.
    """

    validation_paths: List[ValidationPath]
    """
    List of other relevant validation paths.
    """

    ts_validation_paths: Optional[List[ValidationPath]] = None
    """
    List of validation paths relevant for embedded timestamps.
    """

    adobe_revinfo_attr: Optional[asn1_pdf.RevocationInfoArchival] = None
    """
    Preformatted revocation info attribute to include, if requested by the
    settings.
    """

    ocsps_to_embed: List[ocsp.OCSPResponse] = field(default_factory=list)
    """
    List of OCSP responses collected so far.
    """

    crls_to_embed: List[crl.CertificateList] = field(default_factory=list)
    """
    List of CRLS collected so far.
    """

    ac_validation_paths: List[ValidationPath] = field(default_factory=list)
    """
    List of validation paths relevant for embedded attribute certificates.
    """


def _register_mac_on_sig(
    pdf_out: BasePdfFileWriter, sig_obj_ref: generic.IndirectObject
):
    pdf_out.set_custom_trailer_entry(
        pdf_name('/AuthCode'),
        generic.DictionaryObject(
            {
                pdf_name('/MACLocation'): pdf_name('/AttachedToSig'),
                pdf_name('/SigObjRef'): sig_obj_ref,
            }
        ),
    )


class PdfSigningSession:
    """
    .. versionadded:: 0.7.0

    Class modelling a PDF signing session in its initial state.

    The ``__init__`` method is internal API, get an instance using
    :meth:`.PdfSigner.init_signing_session`.
    """

    def __init__(
        self,
        pdf_signer: PdfSigner,
        pdf_out: BasePdfFileWriter,
        cms_writer,
        sig_field,
        md_algorithm: str,
        timestamper: Optional[TimeStamper],
        subfilter: SigSeedSubFilter,
        system_time: Optional[datetime] = None,
        sv_spec: Optional[SigSeedValueSpec] = None,
    ):
        self.pdf_signer = pdf_signer
        self.pdf_out = pdf_out
        self.sig_field = sig_field
        self.cms_writer = cms_writer
        self.md_algorithm = md_algorithm
        self.timestamper = timestamper
        self.subfilter = subfilter
        self.use_pades = subfilter == SigSeedSubFilter.PADES
        self.system_time = system_time or datetime.now(
            tz=tzlocal.get_localzone()
        )
        self.sv_spec = sv_spec

    async def perform_presign_validation(
        self, pdf_out: Optional[BasePdfFileWriter] = None
    ) -> Optional[PreSignValidationStatus]:
        """
        Perform certificate validation checks for the signer's certificate,
        including any necessary revocation checks.

        This function will also attempt to validate & collect revocation
        information for the relevant TSA (by requesting a dummy timestamp).

        :param pdf_out:
            Current PDF writer. Technically optional; only used to look for
            the end of the timestamp chain in the previous revision when
            producing a PAdES-LTA signature in a document that is already
            signed (to ensure that the timestamp chain is uninterrupted).
        :return:
            A :class:`PreSignValidationStatus` object, or ``None`` if there
            is no validation context available.
        """

        pdf_signer = self.pdf_signer
        validation_paths = []
        signature_meta = pdf_signer.signature_meta
        validation_context = signature_meta.validation_context

        if signature_meta.embed_validation_info:
            if self.pdf_signer.signer.signing_cert is None:
                raise SigningError(
                    "A signer's certificate must be provided if "
                    "validation/revocation info is to be embedded into the "
                    "signature."
                )
            elif validation_context is None:
                raise SigningError(
                    "A validation context must be provided if "
                    "validation/revocation info is to be embedded into the "
                    "signature."
                )
            elif not validation_context.fetching_allowed:
                logger.warning(
                    "Validation/revocation info will be embedded, but "
                    "fetching is not allowed. This may give rise to unexpected "
                    "results."
                )
        validation_context = signature_meta.validation_context
        # if there's no validation context, bail early
        if validation_context is None:
            return None

        signer_path = await self._perform_presign_signer_validation(
            validation_context, signature_meta.signer_key_usage
        )
        validation_paths.append(signer_path)

        # If LTA:
        # if the original document already included a document timestamp,
        # we need to collect revocation information for it, to preserve
        # the integrity of the timestamp chain
        if signature_meta.use_pades_lta and isinstance(
            pdf_out, IncrementalPdfFileWriter
        ):
            prev_tsa_path = await self._perform_prev_ts_validation(
                validation_context, pdf_out.prev
            )
            if prev_tsa_path is not None:
                validation_paths.append(prev_tsa_path)

        timestamper = self.timestamper
        # Finally, fetch validation information for the TSA that we're going to
        # use for our own TS
        if timestamper is not None:
            async_ts_paths = timestamper.validation_paths(validation_context)
            ts_paths = []
            async for ts_path in async_ts_paths:
                validation_paths.append(ts_path)
                ts_paths.append(ts_path)
        else:
            ts_paths = None

        aa_paths: List[ValidationPath]
        # fetch attribute certificate validation paths
        if signature_meta.ac_validation_context is not None:
            async_aa_paths = self._perform_presign_ac_validation(
                signature_meta.ac_validation_context
            )
            aa_paths = []
            async for aa_path in async_aa_paths:
                validation_paths.append(aa_path)
                aa_paths.append(aa_path)
        else:
            aa_paths = []

        # do we need adobe-style revocation info?
        if signature_meta.embed_validation_info and not self.use_pades:
            assert validation_context is not None  # checked earlier
            revinfo = Signer.format_revinfo(
                ocsp_responses=validation_context.ocsps,
                crls=validation_context.crls,
            )
        else:
            # PAdES prescribes another mechanism for embedding revocation info
            revinfo = None
        return PreSignValidationStatus(
            validation_paths=validation_paths,
            signer_path=signer_path,
            ts_validation_paths=ts_paths,
            adobe_revinfo_attr=revinfo,
            ocsps_to_embed=validation_context.ocsps,
            crls_to_embed=validation_context.crls,
            ac_validation_paths=aa_paths,
        )

    async def _perform_presign_ac_validation(
        self, validation_context: ValidationContext
    ):
        signer = self.pdf_signer.signer
        attr_certs = list(signer.attribute_certs)
        cades_attr_spec = self.pdf_signer.signature_meta.cades_signed_attr_spec
        # also make sure to pull in the validation chains for all attribute
        # certificates included in the signer-attributes-v2 attr, if there is
        # one.
        if (
            cades_attr_spec is not None
            and cades_attr_spec.signer_attributes is not None
        ):
            attr_certs.extend(cades_attr_spec.signer_attributes.certified_attrs)
        ac_jobs = [
            async_validate_ac(
                ac, validation_context, holder_cert=signer.signing_cert
            )
            for ac in attr_certs
        ]
        for ac_job in asyncio.as_completed(ac_jobs):
            result: ACValidationResult = await ac_job
            yield result.aa_path

    async def _perform_presign_signer_validation(
        self, validation_context, key_usage
    ):
        signer = self.pdf_signer.signer
        # validate cert
        # (this also keeps track of any validation data automagically)
        validator = CertificateValidator(
            signer.signing_cert,
            intermediate_certs=signer.cert_registry,
            validation_context=validation_context,
        )
        try:
            signer_cert_validation_path = await validator.async_validate_usage(
                key_usage
            )
        except (PathBuildingError, PathValidationError) as e:
            raise SigningError(
                "The signer's certificate could not be validated"
            ) from e
        return signer_cert_validation_path

    async def _perform_prev_ts_validation(
        self, validation_context, prev_reader
    ):
        signer = self.pdf_signer.signer
        from pyhanko.sign.validation import get_timestamp_chain

        # try to grab the most recent document timestamp
        last_ts = None
        try:
            last_ts = next(get_timestamp_chain(prev_reader))
        except StopIteration:
            pass
        last_ts_validation_path = None
        if last_ts is not None:
            ts_validator = CertificateValidator(
                last_ts.signer_cert,
                intermediate_certs=signer.cert_registry,
                validation_context=validation_context,
            )
            try:
                validate_coro = ts_validator.async_validate_usage(
                    set(), extended_key_usage={"time_stamping"}
                )
                last_ts_validation_path = await validate_coro
            except (PathBuildingError, PathValidationError) as e:
                raise SigningError(
                    "Requested a PAdES-LTA signature on an existing "
                    "document, but the most recent timestamp "
                    "could not be validated.",
                    e,
                )
        return last_ts_validation_path

    def _apply_locking_rules(self) -> SigMDPSetup:
        # TODO allow equivalent functionality to the /Lock dictionary
        #  to be specified in PdfSignatureMetadata

        # this helper method handles /Lock dictionary and certification
        #  semantics.
        # The fallback rules are messy and ad-hoc; behaviour is mostly
        # documented by tests.

        # read recommendations and/or requirements from the SV dictionary
        sv_spec = self.sv_spec
        sig_field = self.sig_field
        signature_meta = self.pdf_signer.signature_meta
        if sv_spec is not None:
            sv_lock_lut: Dict[Any, List[MDPPerm]] = {
                SeedLockDocument.LOCK: [MDPPerm.NO_CHANGES],
                SeedLockDocument.DO_NOT_LOCK: [
                    MDPPerm.FILL_FORMS,
                    MDPPerm.ANNOTATE,
                ],
            }
            sv_lock_values: List[MDPPerm] = sv_lock_lut.get(
                sv_spec.lock_document, []
            )
            sv_lock_value_req = sv_lock_values is not None and bool(
                sv_spec.flags & SigSeedValFlags.LOCK_DOCUMENT
            )
        else:
            sv_lock_values = []
            sv_lock_value_req = False

        lock = lock_dict = None
        # init the DocMDP value with what the /LockDocument setting in the SV
        # dict recommends. If the constraint is mandatory, it might conflict
        # with the /Lock dictionary, but we'll deal with that later.
        docmdp_perms = sv_lock_values[0] if sv_lock_values else None
        try:
            lock_dict = sig_field['/Lock']
            lock = FieldMDPSpec.from_pdf_object(lock_dict)
            docmdp_value = lock_dict['/P']
            docmdp_perms = MDPPerm(docmdp_value)
            if sv_lock_value_req and docmdp_perms not in sv_lock_values:
                raise SigningError(
                    "Inconsistency in form field data. "
                    "The field lock dictionary imposes the DocMDP policy "
                    f"'{docmdp_perms}', but the seed value "
                    "dictionary's /LockDocument does not allow that."
                )
        except KeyError:
            pass
        except ValueError as e:
            raise SigningError("Failed to read /Lock dictionary", e)

        meta_perms = signature_meta.docmdp_permissions
        meta_certify = signature_meta.certify

        # only pull meta_perms into the validation if we're trying to make a
        # cert sig, or there already is some other docmdp_perms value available.
        # (in other words, if there's no SV dict or /Lock, and we're not
        # certifying, this will be skipped)
        if meta_perms is not None and (
            meta_certify or docmdp_perms is not None
        ):
            if sv_lock_value_req and meta_perms not in sv_lock_values:
                # in this case, we have to override
                docmdp_perms = sv_lock_values[0]
            else:
                # choose the stricter option if both are available
                docmdp_perms = (
                    meta_perms
                    if docmdp_perms is None
                    else (min(docmdp_perms, meta_perms))
                )
            if docmdp_perms != meta_perms:
                logger.warning(
                    f"DocMDP policy '{meta_perms}', was requested, "
                    f"but the signature field settings do "
                    f"not allow that. Setting '{docmdp_perms}' instead."
                )

        # if not certifying and docmdp_perms is not None, ensure the
        # appropriate permission in the Lock dictionary is set
        if not meta_certify and docmdp_perms is not None:
            if lock_dict is None:
                # set a field lock that doesn't do anything
                sig_field['/Lock'] = lock_dict = generic.DictionaryObject(
                    {
                        pdf_name('/Action'): pdf_name('/Include'),
                        pdf_name('/Fields'): generic.ArrayObject(),
                    }
                )
            lock_dict['/P'] = generic.NumberObject(docmdp_perms.value)

        return SigMDPSetup(
            certify=meta_certify,
            field_lock=lock,
            docmdp_perms=docmdp_perms,
            md_algorithm=self.md_algorithm,
        )

    def _enforce_seed_value_constraints(
        self, validation_path: Optional[ValidationPath]
    ):
        sv_spec = self.sv_spec
        pdf_signer = self.pdf_signer
        signature_meta = pdf_signer.signature_meta
        assert sv_spec is not None

        # Enforce mandatory seed values (except LOCK_DOCUMENT, which is handled
        #  elsewhere)
        flags: SigSeedValFlags = sv_spec.flags

        if sv_spec.cert is not None:
            if pdf_signer.signer.signing_cert is None:
                raise SigningError(
                    "Cannot verify seed value constraints on the signer's "
                    "certificate since it is not available"
                )
            sv_spec.cert.satisfied_by(
                pdf_signer.signer.signing_cert, validation_path
            )

        if sv_spec.seed_signature_type is not None:
            sv_certify = sv_spec.seed_signature_type.certification_signature()
            if sv_certify != signature_meta.certify:

                def _type(certify):
                    return 'a certification' if certify else 'an approval'

                raise SigningError(
                    "The seed value dictionary's /MDP entry specifies that "
                    f"this field should contain {_type(sv_certify)} "
                    f"signature, but {_type(signature_meta.certify)} "
                    "was requested."
                )
            sv_mdp_perm = sv_spec.seed_signature_type.mdp_perm
            if sv_certify and sv_mdp_perm != signature_meta.docmdp_permissions:
                raise SigningError(
                    "The seed value dictionary specified that this "
                    "certification signature should use the MDP policy "
                    f"'{sv_mdp_perm}', "
                    f"but '{signature_meta.docmdp_permissions}' was "
                    "requested."
                )

        if not flags:
            return sv_spec

        selected_sf = signature_meta.subfilter
        if (
            flags & SigSeedValFlags.SUBFILTER
        ) and sv_spec.subfilters is not None:
            # empty array = no supported subfilters
            if not sv_spec.subfilters:
                raise NotImplementedError(
                    "The signature encodings mandated by the seed value "
                    "dictionary are not supported."
                )
            # standard mandates that we take the first available subfilter
            mandated_sf: SigSeedSubFilter = sv_spec.subfilters[0]
            if selected_sf is not None and mandated_sf != selected_sf:
                raise SigningError(
                    "The seed value dictionary mandates subfilter '%s', "
                    "but '%s' was requested."
                    % (mandated_sf.value, selected_sf.value)
                )

        # SV dict serves as a source of defaults as well
        if selected_sf is None and sv_spec.subfilters is not None:
            selected_sf = sv_spec.subfilters[0]

        if (
            flags & SigSeedValFlags.APPEARANCE_FILTER
        ) and sv_spec.appearance is not None:
            raise SigningError(
                "pyHanko does not define any named appearances, but "
                "the seed value dictionary requires that the named appearance "
                f"'{sv_spec.appearance}' be used."
            )

        if (
            flags & SigSeedValFlags.ADD_REV_INFO
        ) and sv_spec.add_rev_info is not None:
            if sv_spec.add_rev_info != signature_meta.embed_validation_info:
                raise SigningError(
                    "The seed value dict mandates that revocation info %sbe "
                    "added; adjust PdfSignatureMetadata settings accordingly."
                    % ("" if sv_spec.add_rev_info else "not ")
                )
            if (
                sv_spec.add_rev_info
                and selected_sf != SigSeedSubFilter.ADOBE_PKCS7_DETACHED
            ):
                raise SigningError(
                    "The seed value dict mandates that Adobe-style revocation "
                    "info be added; this requires subfilter '%s'"
                    % (SigSeedSubFilter.ADOBE_PKCS7_DETACHED.value)
                )
        if (
            flags & SigSeedValFlags.DIGEST_METHOD
        ) and sv_spec.digest_methods is not None:
            selected_md = pdf_signer.default_md_for_signer
            if selected_md is not None:
                selected_md = selected_md.lower()
                if selected_md not in sv_spec.digest_methods:
                    raise SigningError(
                        "The selected message digest %s is not allowed by the "
                        "seed value dictionary. Please select one of %s."
                        % (selected_md, ", ".join(sv_spec.digest_methods))
                    )

        if flags & SigSeedValFlags.REASONS:
            reasons = sv_spec.reasons
            # standard says that omission of the /Reasons key amounts to
            #  a prohibition in this case
            must_omit = not reasons or reasons == ["."]
            reason_given = signature_meta.reason
            if must_omit and reason_given is not None:
                raise SigningError(
                    "The seed value dictionary prohibits giving a reason "
                    "for signing."
                )
            if not must_omit and reasons and reason_given not in reasons:
                raise SigningError(
                    "Reason \"%s\" is not a valid reason for signing, "
                    "please choose one of the following: %s."
                    % (
                        reason_given,
                        ", ".join("\"%s\"" % s for s in reasons),
                    )
                )

    async def estimate_signature_container_size(
        self, validation_info: Optional[PreSignValidationStatus], tight=False
    ):
        md_algorithm = self.md_algorithm
        signature_meta = self.pdf_signer.signature_meta
        signer = self.pdf_signer.signer

        if signer.signing_cert is None:
            raise SigningError(
                "Automatic signature size estimation is not available without "
                "a signer's certificate. Space must be allocated manually "
                "using bytes_reserved=..."
            )
        # estimate bytes_reserved by creating a fake CMS object
        md_spec = get_pyca_cryptography_hash(md_algorithm)
        test_md = hashes.Hash(md_spec).finalize()
        signed_attrs = PdfCMSSignedAttributes(
            signing_time=self.system_time,
            adobe_revinfo_attr=(
                None
                if validation_info is None
                else validation_info.adobe_revinfo_attr
            ),
            cades_signed_attrs=signature_meta.cades_signed_attr_spec,
        )
        test_signature_cms = await signer.async_sign(
            test_md,
            md_algorithm,
            use_pades=self.use_pades,
            dry_run=True,
            timestamper=self.timestamper,
            signed_attr_settings=signed_attrs,
        )

        # Note: multiply by 2 to account for the fact that this byte dump
        # will be embedded into the resulting PDF as a hexadecimal
        # string
        test_len = len(test_signature_cms.dump()) * 2

        if tight:
            bytes_reserved = test_len
        else:
            # External actors such as timestamping servers can't be relied on to
            # always return exactly the same response, so we build in a 50%
            # error margin (+ ensure that bytes_reserved is even)
            bytes_reserved = test_len + 2 * (test_len // 4)
        return bytes_reserved

    def prepare_tbs_document(
        self,
        validation_info: Optional[PreSignValidationStatus],
        bytes_reserved,
        appearance_text_params=None,
    ) -> 'PdfTBSDocument':
        """
        Set up the signature appearance (if necessary) and signature dictionary
        in the PDF file, to put the document in its final pre-signing state.

        :param validation_info:
            Validation information collected prior to signing.
        :param bytes_reserved:
            Bytes to reserve for the signature container.
        :param appearance_text_params:
            Optional text parameters for the signature appearance content.
        :return:
            A :class:`.PdfTBSDocument` describing the document in its final
            pre-signing state.
        """

        pdf_signer = self.pdf_signer
        signature_meta = self.pdf_signer.signature_meta
        if self.sv_spec is not None:
            # process the field's seed value constraints
            self._enforce_seed_value_constraints(
                None if validation_info is None else validation_info.signer_path
            )

        signer = pdf_signer.signer
        embed_roots = signer.embed_roots
        # take care of DSS updates, if they have to happen now
        dss_settings = signature_meta.dss_settings
        if self.use_pades and validation_info is not None:
            # Check consistency of settings
            dss_settings.assert_viable()
            if (
                dss_settings.placement
                == SigDSSPlacementPreference.TOGETHER_WITH_SIGNATURE
            ):
                from pyhanko.sign import validation

                pdf_out = self.pdf_out
                # source info directly from the validation_info object
                # for consistency
                # NOTE: we have to disable VRI in this scenario
                validation.DocumentSecurityStore.supply_dss_in_writer(
                    pdf_out,
                    sig_contents=None,
                    paths=validation_info.validation_paths,
                    ocsps=validation_info.ocsps_to_embed,
                    crls=validation_info.crls_to_embed,
                    embed_roots=embed_roots,
                )

        md_algorithm = self.md_algorithm

        sig_mdp_setup = self._apply_locking_rules()

        # Prepare instructions to the CMS writer to set up the
        # (PDF) signature object and its appearance
        system_time = self.system_time
        name_specified = signature_meta.name
        sig_appearance = SigAppearanceSetup(
            style=pdf_signer.stamp_style,
            name=name_specified or signer.subject_name,
            timestamp=system_time,
            text_params=appearance_text_params,
        )

        sig_obj = SignatureObject(
            bytes_reserved=bytes_reserved,
            subfilter=self.subfilter,
            timestamp=system_time,
            name=name_specified if name_specified else None,
            location=signature_meta.location,
            reason=signature_meta.reason,
            contact_info=signature_meta.contact_info,
            prop_auth_time=signature_meta.prop_auth_time,
            prop_auth_type=signature_meta.prop_auth_type,
            app_build_props=signature_meta.app_build_props,
        )

        # Pass in the SignatureObject settings
        sig_obj_ref = self.cms_writer.send(
            SigObjSetup(
                sig_placeholder=sig_obj,
                mdp_setup=sig_mdp_setup,
                appearance_setup=sig_appearance,
            )
        )

        signer = pdf_signer.signer
        security_handler = self.pdf_out.security_handler
        if security_handler is not None and security_handler.pdf_mac_enabled:
            _register_mac_on_sig(self.pdf_out, sig_obj_ref)
            PdfMacAttrProviderSpec(self.pdf_out).install(
                signer, self.timestamper
            )

        # At this point, the document is in its final pre-signing state

        # Last job: prepare instructions for the post-signing workflow
        signature_meta = pdf_signer.signature_meta
        validation_context = signature_meta.validation_context
        post_signing_instr = doc_timestamper = None
        if self.use_pades and signature_meta.embed_validation_info:
            if validation_info is None:
                raise SigningError(
                    "Requested embedding of validation info, "
                    "but none was collected"
                )
            if signature_meta.use_pades_lta:
                doc_timestamper = self.timestamper
            # if necessary/supported, extract a file access credential
            # to perform post-signing operations later
            credential_ser: Optional[SerialisedCredential] = None
            if security_handler is not None:
                credential = security_handler.extract_credential()
                if credential is not None:
                    credential_ser = credential.serialise()
            post_signing_instr = PostSignInstructions(
                validation_info=validation_info,
                # use the same algorithm
                # TODO make this configurable? Some TSAs only allow one choice
                #  of MD, and forcing our signers to use the same one to handle
                #  might be overly restrictive (esp. for things like EdDSA where
                #  the MD is essentially fixed)
                timestamp_md_algorithm=md_algorithm,
                timestamper=doc_timestamper,
                timestamp_field_name=signature_meta.timestamp_field_name,
                dss_settings=signature_meta.dss_settings,
                tight_size_estimates=signature_meta.tight_size_estimates,
                embed_roots=embed_roots,
                file_credential=credential_ser,
            )
        return PdfTBSDocument(
            cms_writer=self.cms_writer,
            signer=signer,
            md_algorithm=md_algorithm,
            timestamper=self.timestamper,
            use_pades=self.use_pades,
            post_sign_instructions=post_signing_instr,
            validation_context=validation_context,
        )


@dataclass(frozen=True)
class PostSignInstructions:
    """
    .. versionadded:: 0.7.0

    Container class housing instructions for incremental updates
    to the document after the signature has been put in place.
    Necessary for PAdES-LT and PAdES-LTA workflows.
    """

    validation_info: PreSignValidationStatus
    """
    Validation information to embed in the DSS (if not already present).
    """

    timestamper: Optional[TimeStamper] = None
    """
    Timestamper to use for produce document timestamps. If ``None``, no
    timestamp will be added.
    """

    timestamp_md_algorithm: Optional[str] = None
    """
    Digest algorithm to use when producing timestamps.
    Defaults to :const:`~pyhanko.sign.signers.constants.DEFAULT_MD`.
    """

    timestamp_field_name: Optional[str] = None
    """
    Name of the timestamp field to use. If not specified, a field name will be
    generated.
    """

    dss_settings: DSSContentSettings = DSSContentSettings()
    """
    .. versionadded:: 0.8.0

    Settings to fine-tune DSS generation.
    """

    tight_size_estimates: bool = False
    """
    .. versionadded:: 0.8.0

    When estimating the size of a document timestamp container,
    do not add safety margins.

    .. note::
        External TSAs cannot be relied upon to always produce the
        exact same output length, which makes this option risky to use.
    """

    embed_roots: bool = True
    """
    .. versionadded:: 0.9.0

    Option that controls whether the root certificate of each validation
    path should be embedded into the DSS. The default is ``True``.

    .. note::
        Trust roots are configured by the validator, so embedding them
        typically does nothing in a typical validation process.
        Therefore they can be safely omitted in most cases.
        Nonetheless, embedding the roots can be useful for documentation
        purposes.

    .. note::
        This setting is not part of :class:`.DSSContentSettings` because
        its value is taken from the corresponding property on the
        :class:`.Signer` involved, not from the initial configuration.
    """

    file_credential: Optional[SerialisedCredential] = None
    """
    .. versionadded:: 0.13.0

    Serialised file credential, to update encrypted files.
    """


class PdfMacAttrProvider(attributes.CMSAttributeProvider):
    attribute_type = 'pdf_mac_data'

    def __init__(
        self,
        *,
        document_digest: bytes,
        signature_digest: bytes,
        mac_handler: 'pdfmac.PdfMacTokenHandler',
    ):
        self.document_digest = document_digest
        self.signature_digest = signature_digest
        self.mac_handler = mac_handler

    async def build_attr_value(self, dry_run=False):
        return self.mac_handler.build_pdfmac_token(
            document_digest=self.document_digest,
            signature_digest=self.signature_digest,
            dry_run=dry_run,
        )


class PdfMacAttrProviderSpec(attributes.UnsignedAttributeProviderSpec):
    """
    Internal handler to deal with signatures under ISO/TS 32004.
    """

    def __init__(
        self,
        pdf_out: BasePdfFileWriter,
        document_digest: Optional[bytes] = None,
    ):
        self.pdf_out = pdf_out
        self.document_digest = document_digest
        self.wrapped: Optional[attributes.UnsignedAttributeProviderSpec] = None
        self._orig: Optional[attributes.UnsignedAttributeProviderSpec] = None
        self._target: Optional[Signer] = None

    def install(self, signer: Signer, timestamper: Optional[TimeStamper]):
        """
        Install the provider on a signer.

        NOTE: this is a stateful operation and won't work
        if the signer is shared, but we have a safeguard in place
        for that (will fail during size estimation under normal
        circumstances).
        """
        # TODO find a way to handle this set/reset process more cleanly,
        #  and/or expose more control. Would wrapping Signer be easier
        #  in practice? It's cleaner in theory, but Signer is a bad mix
        #  between extensible API and structural logic, so that might have to
        #  wait.
        assert self._orig is None
        self._target = signer
        self._orig = signer.unsigned_attr_prov_spec
        self.wrapped = signer._unsigned_attr_provider_spec(timestamper)
        signer.unsigned_attr_prov_spec = self

    def uninstall(self):
        """
        Remove the provider from its target (internal API).
        """
        self._target.unsigned_attr_prov_spec = self._orig

    def unsigned_attr_providers(
        self,
        signature: bytes,
        signed_attrs: cms.CMSAttributes,
        digest_algorithm: str,
    ) -> Iterable[CMSAttributeProvider]:
        assert self.wrapped is not None, "Unsigned attr provider not installed"
        other_provs = self.wrapped.unsigned_attr_providers(
            signature, signed_attrs, digest_algorithm
        )

        for prov in other_provs:
            if isinstance(prov, PdfMacAttrProvider):
                raise SigningError(
                    "Other PDF MAC attribute provider found. This may be the "
                    "result of signer instances being shared across multiple "
                    "documents. This workflow is not supported for encrypted "
                    "documents. Either turn off MAC generation, or use "
                    "separate signer instances for different documents."
                )
            yield prov

        md = hashes.Hash(get_pyca_cryptography_hash(digest_algorithm))
        md.update(signature)
        sig_digest = md.finalize()

        document_digest = self.document_digest
        if not document_digest:
            # since by default we're aligning the hash function with that of
            # the underlying signature anyhow,
            # we can simply "steal" the value of the messageDigest attribute
            document_digest = find_unique_cms_attribute(
                signed_attrs, 'message_digest'
            ).native
        assert document_digest is not None
        yield PdfMacAttrProvider(
            document_digest=document_digest,
            signature_digest=sig_digest,
            mac_handler=self.pdf_out._init_mac_handler(digest_algorithm),
        )


class PdfTBSDocument:
    """
    .. versionadded:: 0.7.0

    A PDF document in its final pre-signing state.

    The ``__init__`` method is internal API, get an instance using
    :meth:`.PdfSigningSession.prepare_tbs_document`. Alternatively, use
    :meth:`resume_signing` or :meth:`finish_signing` to continue a previously
    interrupted signing process without instantiating a new
    :class:`.PdfTBSDocument` object.
    """

    def __init__(
        self,
        cms_writer,
        signer: Signer,
        md_algorithm: str,
        use_pades: bool,
        timestamper: Optional[TimeStamper] = None,
        post_sign_instructions: Optional[PostSignInstructions] = None,
        validation_context: Optional[ValidationContext] = None,
    ):
        self.cms_writer = cms_writer
        self.signer = signer
        self.md_algorithm = md_algorithm
        self.timestamper = timestamper
        self.use_pades = use_pades
        self.post_sign_instructions = post_sign_instructions
        self.validation_context = validation_context

    def digest_tbs_document(
        self,
        *,
        output: Optional[IO] = None,
        in_place: bool = False,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
    ) -> Tuple[PreparedByteRangeDigest, IO]:
        """
        Write the document to an output stream and compute the digest, while
        keeping track of the (future) location of the signature contents in the
        output stream.

        The digest can then be passed to the next part of the signing pipeline.

        .. warning::
            This method can only be called once.

        :param output:
            Write the output to the specified output stream.
            If ``None``, write to a new :class:`.BytesIO` object.
            Default is ``None``.
        :param in_place:
            Sign the original input stream in-place.
            This parameter overrides ``output``.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        :return:
            A tuple containing a :class:`.PreparedByteRangeDigest` and the
            output stream to which the output was written.
        """

        # pass in I/O parameters, get back a hash
        return self.cms_writer.send(
            SigIOSetup(
                md_algorithm=self.md_algorithm,
                in_place=in_place,
                chunk_size=chunk_size,
                output=output,
            )
        )

    async def perform_signature(
        self,
        document_digest: bytes,
        pdf_cms_signed_attrs: PdfCMSSignedAttributes,
    ) -> 'PdfPostSignatureDocument':
        """
        Perform the relevant cryptographic signing operations on the document
        digest, and write the resulting CMS object to the appropriate location
        in the output stream.

        .. warning::
            This method can only be called once, and must be invoked after
            :meth:`digest_tbs_document`.

        :param document_digest:
            Digest of the document, as computed over the relevant
            ``/ByteRange``.
        :param pdf_cms_signed_attrs:
            Description of the signed attributes to include.
        :return:
            A :class:`.PdfPostSignatureDocument` object.
        """
        signer = self.signer
        signature_cms = await signer.async_sign(
            document_digest,
            self.md_algorithm,
            use_pades=self.use_pades,
            timestamper=self.timestamper,
            signed_attr_settings=pdf_cms_signed_attrs,
        )
        # ... and feed it to the CMS writer
        sig_contents = self.cms_writer.send(signature_cms)

        # lastly, restore the signer
        if isinstance(signer.unsigned_attr_prov_spec, PdfMacAttrProviderSpec):
            signer.unsigned_attr_prov_spec.uninstall()
        return PdfPostSignatureDocument(
            sig_contents,
            post_sign_instr=self.post_sign_instructions,
            validation_context=self.validation_context,
        )

    @classmethod
    def resume_signing(
        cls,
        output: IO,
        prepared_digest: PreparedByteRangeDigest,
        signature_cms: Union[bytes, cms.ContentInfo],
        post_sign_instr: Optional[PostSignInstructions] = None,
        validation_context: Optional[ValidationContext] = None,
    ) -> 'PdfPostSignatureDocument':
        """
        Resume signing after obtaining a CMS object from an external source.

        This is a class method; it doesn't require a :class:`.PdfTBSDocument`
        instance. Contrast with :meth:`perform_signature`.

        :param output:
            Output stream housing the document in its final pre-signing state.
            This stream must at least be writable and seekable, and also
            readable if post-signature processing is required.
        :param prepared_digest:
            The prepared digest returned by a prior call to
            :meth:`digest_tbs_document`.
        :param signature_cms:
            CMS object to embed in the signature dictionary.
        :param post_sign_instr:
            Instructions for post-signing processing (DSS updates and document
            timestamps).
        :param validation_context:
            Validation context to use in post-signing operations.
            This is mainly intended for TSA certificate validation, but it can
            also contain additional validation data to embed in the DSS.
        :return:
            A :class:`PdfPostSignatureDocument`.
        """

        sig_contents = prepared_digest.fill_with_cms(output, signature_cms)
        return PdfPostSignatureDocument(
            sig_contents,
            post_sign_instr=post_sign_instr,
            validation_context=validation_context,
        )

    @classmethod
    def finish_signing(
        cls,
        output: IO,
        prepared_digest: PreparedByteRangeDigest,
        signature_cms: Union[bytes, cms.ContentInfo],
        post_sign_instr: Optional[PostSignInstructions] = None,
        validation_context: Optional[ValidationContext] = None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
    ):
        """
        Finish signing after obtaining a CMS object from an external source, and
        perform any required post-signature processing.

        This is a class method; it doesn't require a :class:`.PdfTBSDocument`
        instance. Contrast with :meth:`perform_signature`.

        :param output:
            Output stream housing the document in its final pre-signing state.
        :param prepared_digest:
            The prepared digest returned by a prior call to
            :meth:`digest_tbs_document`.
        :param signature_cms:
            CMS object to embed in the signature dictionary.
        :param post_sign_instr:
            Instructions for post-signing processing (DSS updates and document
            timestamps).
        :param validation_context:
            Validation context to use in post-signing operations.
            This is mainly intended for TSA certificate validation, but it can
            also contain additional validation data to embed in the DSS.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        """
        asyncio.run(
            cls.async_finish_signing(
                output,
                prepared_digest,
                signature_cms,
                post_sign_instr=post_sign_instr,
                validation_context=validation_context,
                chunk_size=chunk_size,
            )
        )

    @classmethod
    async def async_finish_signing(
        cls,
        output: IO,
        prepared_digest: PreparedByteRangeDigest,
        signature_cms: Union[bytes, cms.ContentInfo],
        post_sign_instr: Optional[PostSignInstructions] = None,
        validation_context: Optional[ValidationContext] = None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
    ):
        """
        Finish signing after obtaining a CMS object from an external source, and
        perform any required post-signature processing.

        This is a class method; it doesn't require a :class:`.PdfTBSDocument`
        instance. Contrast with :meth:`perform_signature`.

        :param output:
            Output stream housing the document in its final pre-signing state.
        :param prepared_digest:
            The prepared digest returned by a prior call to
            :meth:`digest_tbs_document`.
        :param signature_cms:
            CMS object to embed in the signature dictionary.
        :param post_sign_instr:
            Instructions for post-signing processing (DSS updates and document
            timestamps).
        :param validation_context:
            Validation context to use in post-signing operations.
            This is mainly intended for TSA certificate validation, but it can
            also contain additional validation data to embed in the DSS.
        :param chunk_size:
            Size of the internal buffer (in bytes) used to feed data to the
            message digest function if the input stream does not support
            ``memoryview``.
        """
        # TODO at this point, the output stream no longer needs to be readable,
        #  just seekable, unless there's a timestamp requirement.
        #  Might want to factor that out for speed at some point.
        rw_output = misc.prepare_rw_output_stream(output)
        post_sign = cls.resume_signing(
            rw_output,
            prepared_digest=prepared_digest,
            signature_cms=signature_cms,
            post_sign_instr=post_sign_instr,
            validation_context=validation_context,
        )
        await post_sign.post_signature_processing(
            rw_output, chunk_size=chunk_size
        )


class PdfPostSignatureDocument:
    """
    .. versionadded:: 0.7.0

    Represents the final phase of the PDF signing process
    """

    def __init__(
        self,
        sig_contents: bytes,
        post_sign_instr: Optional[PostSignInstructions] = None,
        validation_context: Optional[ValidationContext] = None,
    ):
        self.sig_contents = sig_contents
        self.post_sign_instructions = post_sign_instr
        self.validation_context = validation_context

    async def post_signature_processing(
        self, output: IO, chunk_size=misc.DEFAULT_CHUNK_SIZE
    ):
        """
        Handle DSS updates and LTA timestamps, if applicable.

        :param output:
            I/O buffer containing the signed document. Must support
            reading, writing and seeking.
        :param chunk_size:
            Chunk size to use for I/O operations that do not support the buffer
            protocol.
        """

        instr = self.post_sign_instructions
        if instr is None:
            return

        validation_context = self.validation_context
        validation_info = instr.validation_info
        dss_settings = instr.dss_settings

        from pyhanko.sign import validation

        # If we're resuming a signing operation, the (new) validation context
        # might not have all relevant OCSP responses / CRLs available.
        # Hence why we also pass in the data from the pre-signing check.
        # The DSS handling code will deal with deduplication.
        dss_op_kwargs: Dict[str, Any]
        dss_op_kwargs = dict(
            paths=validation_info.validation_paths,
            validation_context=validation_context,
            ocsps=validation_info.ocsps_to_embed,
            crls=validation_info.crls_to_embed,
            embed_roots=instr.embed_roots,
        )
        if dss_settings.include_vri:
            dss_op_kwargs['sig_contents'] = self.sig_contents
        else:
            dss_op_kwargs['sig_contents'] = None

        timestamper = instr.timestamper
        # Separate DSS revision if no TS that would otherwise be bundled with it
        # or explicitly requested as separate
        dss_placement = dss_settings.placement
        separate_dss_revision = False
        if dss_placement == SigDSSPlacementPreference.SEPARATE_REVISION:
            separate_dss_revision = True
        elif dss_placement == SigDSSPlacementPreference.TOGETHER_WITH_NEXT_TS:
            separate_dss_revision = timestamper is None
        if separate_dss_revision:
            if not dss_settings.skip_if_unneeded:
                dss_op_kwargs['force_write'] = True
            validation.DocumentSecurityStore.add_dss(
                output_stream=output,
                **dss_op_kwargs,
                file_credential=instr.file_credential,
            )
        if timestamper is not None:
            # append a document timestamp after the DSS update
            w = IncrementalPdfFileWriter(output)
            if (
                w.security_handler is not None
                and instr.file_credential is not None
            ):
                w.security_handler.authenticate(instr.file_credential)
                # we let the SH throw errors on access as necessary
            pdf_timestamper = PdfTimeStamper(
                timestamper, field_name=instr.timestamp_field_name
            )
            if dss_placement == SigDSSPlacementPreference.TOGETHER_WITH_NEXT_TS:
                validation.DocumentSecurityStore.supply_dss_in_writer(
                    w, **dss_op_kwargs
                )
            await pdf_timestamper.async_timestamp_pdf(
                w,
                instr.timestamp_md_algorithm or constants.DEFAULT_MD,
                validation_context,
                validation_paths=validation_info.validation_paths,
                in_place=True,
                timestamper=timestamper,
                chunk_size=chunk_size,
                dss_settings=dss_settings.get_settings_for_ts(),
                tight_size_estimates=instr.tight_size_estimates,
                embed_roots=instr.embed_roots,
            )
