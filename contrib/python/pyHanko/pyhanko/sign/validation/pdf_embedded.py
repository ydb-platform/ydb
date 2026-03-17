import logging
import os
from collections import namedtuple
from datetime import datetime
from typing import List, Optional, Union

from asn1crypto import cms, x509
from pyhanko_certvalidator import ValidationContext
from pyhanko_certvalidator.path import ValidationPath

from pyhanko.pdf_utils import generic, misc
from pyhanko.pdf_utils.generic import pdf_name
from pyhanko.pdf_utils.reader import PdfFileReader, process_data_at_eof
from pyhanko.sign.diff_analysis import (
    DEFAULT_DIFF_POLICY,
    DiffPolicy,
    DiffResult,
    ModificationLevel,
    SuspiciousModification,
)
from pyhanko.sign.fields import (
    FieldMDPSpec,
    MDPPerm,
    SeedLockDocument,
    SigSeedSubFilter,
    SigSeedValFlags,
    SigSeedValueSpec,
)
from pyhanko.sign.general import (
    SignedDataCerts,
    UnacceptableSignerError,
    byte_range_digest,
    extract_signer_info,
)

from .errors import (
    SignatureValidationError,
    SigSeedValueValidationError,
    ValidationInfoReadingError,
)
from .generic_cms import (
    cms_basic_validation,
    collect_signer_attr_status,
    collect_timing_info,
    compute_signature_tst_digest,
    extract_certs_for_validation,
    extract_self_reported_ts,
    extract_tst_data,
    validate_tst_signed_data,
)
from .settings import KeyUsageConstraints
from .status import (
    DocumentTimestampStatus,
    PdfSignatureStatus,
    SignatureCoverageLevel,
)
from .utils import CMSAlgorithmUsagePolicy

__all__ = [
    'EmbeddedPdfSignature',
    'DocMDPInfo',
    'read_certification_data',
    'async_validate_pdf_signature',
    'async_validate_pdf_timestamp',
    'report_seed_value_validation',
    'extract_contents',
]


logger = logging.getLogger(__name__)


def _extract_reference_dict(
    signature_obj, method
) -> Optional[generic.DictionaryObject]:
    try:
        sig_refs = signature_obj['/Reference']
    except KeyError:
        return None
    for ref in sig_refs:
        ref = ref.get_object()
        if ref['/TransformMethod'] == method:
            return ref
    return None


def _extract_docmdp_for_sig(signature_obj) -> Optional[MDPPerm]:
    ref = _extract_reference_dict(signature_obj, '/DocMDP')
    if ref is None:
        return None
    try:
        raw_perms = ref['/TransformParams'].raw_get('/P')
        return MDPPerm(raw_perms)
    except (ValueError, KeyError) as e:  # pragma: nocover
        raise SignatureValidationError(
            "Failed to read document permissions"
        ) from e


def extract_contents(sig_object: generic.DictionaryObject) -> bytes:
    """
    Internal function to extract the (DER-encoded) signature bytes from a PDF
    signature dictionary.

    :param sig_object:
        A signature dictionary.
    :return:
        The extracted contents as a byte string.
    """

    try:
        cms_content = sig_object.raw_get(
            '/Contents', decrypt=generic.EncryptedObjAccess.RAW
        )
    except KeyError:
        raise misc.PdfReadError('Could not read /Contents entry in signature')

    if not isinstance(
        cms_content, (generic.TextStringObject, generic.ByteStringObject)
    ):
        raise misc.PdfReadError('/Contents must be string-like')
    return cms_content.original_bytes


# TODO clarify in docs that "external timestamp" is always None when dealing
#  with a /DocTimeStamp, since there the timestamp token is simply the entire
#  signature object
class EmbeddedPdfSignature:
    """
    Class modelling a signature embedded in a PDF document.
    """

    sig_field: generic.DictionaryObject
    """
    The field dictionary of the form field containing the signature.
    """

    sig_object: generic.DictionaryObject
    """
    The signature dictionary.
    """

    signed_data: cms.SignedData
    """
    CMS signed data in the signature.
    """

    def __init__(
        self,
        reader: PdfFileReader,
        sig_field: generic.DictionaryObject,
        fq_name: str,
    ):
        self.reader = reader
        if isinstance(sig_field, generic.IndirectObject):
            sig_field = sig_field.get_object()
        self.sig_field = sig_field
        sig_object_ref = sig_field.raw_get('/V')
        self.sig_object = sig_object = sig_object_ref.get_object()
        assert isinstance(sig_object, generic.DictionaryObject)
        try:
            self.byte_range = sig_object.raw_get('/ByteRange')
        except KeyError:
            raise misc.PdfReadError(
                'Could not read /ByteRange entry in signature'
            )
        self.pkcs7_content = cms_content = extract_contents(sig_object)

        message = cms.ContentInfo.load(cms_content)
        signed_data = message['content']
        self.signed_data: cms.SignedData = signed_data

        self.signer_info = extract_signer_info(signed_data)
        self._sd_cert_info: Optional[SignedDataCerts] = None

        # The PDF standard does not define a way to specify the digest algorithm
        # used other than this one.
        # However, RFC 5652 § 11.2 states that the message_digest attribute
        # (which in our case is the PDF's ByteRange digest) is to be computed
        # using the signer's digest algorithm. This can only refer
        # to the corresponding SignerInfo entry.
        digest_algo = self.signer_info['digest_algorithm']
        self.md_algorithm = digest_algo['algorithm'].native.lower()
        eci = signed_data['encap_content_info']
        content_type = eci['content_type'].native
        if content_type == 'data':
            # Case of a normal signature
            self.external_md_algorithm = self.md_algorithm
        elif content_type == 'tst_info':
            # for timestamps, the hash algorithm in the messageImprint
            # need not be the same as the one to digest the encapsulated data!
            # RFC 8933 recommends to unify them, but it's not a given.
            mi = eci['content'].parsed['message_imprint']
            self.external_md_algorithm = mi['hash_algorithm'][
                'algorithm'
            ].native

        # grab the revision to which the signature applies
        # NOTE: We're using get_last_change here as opposed to
        # get_introducing_revision. The distinction won't be relevant in most
        # legitimate use cases, but get_last_change is more likely to be correct
        # in cases where the signature obj was created by overriding an existing
        # object (which is weird, but technically possible, I guess).
        # Important note: the coverage checker will validate whether the
        # xref table for that revision is actually covered by the signature,
        # and raise the alarm if that's not the case.
        # Therefore shenanigans with updating signature objects will be detected
        # even before the diff checker runs.
        self.signed_revision = self.reader.xrefs.get_last_change(
            sig_object_ref.reference
        )
        self.coverage = None
        self.external_digest: Optional[bytes] = None
        self.total_len: Optional[int] = None
        self._docmdp: Optional[MDPPerm] = None
        self._fieldmdp: Optional[FieldMDPSpec] = None
        self._docmdp_queried = self._fieldmdp_queried = False
        self.tst_signature_digest: Optional[bytes] = None

        self.diff_result = None
        self._integrity_checked = False
        self.fq_name = fq_name

    def _init_cert_info(self) -> SignedDataCerts:
        if self._sd_cert_info is None:
            self._sd_cert_info = extract_certs_for_validation(self.signed_data)
        return self._sd_cert_info

    @property
    def embedded_attr_certs(self) -> List[cms.AttributeCertificateV2]:
        """
        Embedded attribute certificates.
        """
        return list(self._init_cert_info().attribute_certs)

    @property
    def other_embedded_certs(self) -> List[x509.Certificate]:
        """
        Embedded X.509 certificates, excluding than that of the signer.
        """
        return list(self._init_cert_info().other_certs)

    @property
    def signer_cert(self) -> x509.Certificate:
        """
        Certificate of the signer.
        """
        return self._init_cert_info().signer_cert

    @property
    def sig_object_type(self) -> generic.NameObject:
        """
        Returns the type of the embedded signature object.
        For ordinary signatures, this will be ``/Sig``.
        In the case of a document timestamp, ``/DocTimeStamp`` is returned.

        :return:
            A PDF name object describing the type of signature.
        """
        return self.sig_object.get('/Type', pdf_name('/Sig'))

    @property
    def field_name(self):
        """
        :return:
            Name of the signature field.
        """
        return self.fq_name

    @property
    def self_reported_timestamp(self) -> Optional[datetime]:
        """
        :return:
            The signing time as reported by the signer, if embedded in the
            signature's signed attributes or provided as part of the signature
            object in the PDF document.
        """
        ts = extract_self_reported_ts(self.signer_info)
        if ts is not None:
            return ts

        try:
            st_as_pdf_date = self.sig_object['/M']
            return generic.parse_pdf_date(
                st_as_pdf_date, strict=self.reader.strict
            )
        except KeyError:  # pragma: nocover
            return None

    @property
    def attached_timestamp_data(self) -> Optional[cms.SignedData]:
        """
        :return:
            The signed data component of the timestamp token embedded in this
            signature, if present.
        """
        return extract_tst_data(self.signer_info)

    def compute_integrity_info(self, diff_policy=None, skip_diff=False):
        """
        Compute the various integrity indicators of this signature.

        :param diff_policy:
            Policy to evaluate potential incremental updates that were appended
            to the signed revision of the document.
            Defaults to
            :const:`~pyhanko.sign.diff_analysis.DEFAULT_DIFF_POLICY`.
        :param skip_diff:
            If ``True``, skip the difference analysis step entirely.
        """
        self._enforce_hybrid_xref_policy()
        self.compute_digest()
        self.compute_tst_digest()

        # TODO in scenarios where we have to verify multiple signatures, we're
        #  doing a lot of double work here. This could be improved.
        self.coverage = self.evaluate_signature_coverage()
        diff_policy = diff_policy or DEFAULT_DIFF_POLICY
        if not skip_diff:
            self.diff_result = self.evaluate_modifications(diff_policy)

        self._integrity_checked = True

    def summarise_integrity_info(self) -> dict:
        """
        Compile the integrity information for this signature into a dictionary
        that can later be passed to :class:`.PdfSignatureStatus` as kwargs.

        This method is only available after calling
        :meth:`.EmbeddedPdfSignature.compute_integrity_info`.
        """

        if not self._integrity_checked:
            raise SignatureValidationError(
                "Call compute_integrity_info() before invoking"
                "summarise_integrity_info()"
            )  # pragma: nocover

        docmdp = self.docmdp_level
        diff_result = self.diff_result
        coverage = self.coverage
        docmdp_ok = None

        # attempt to set docmdp_ok based on the diff analysis results
        if diff_result is not None:
            mod_level = (
                diff_result.modification_level
                if isinstance(diff_result, DiffResult)
                else ModificationLevel.OTHER
            )
            docmdp_ok = not (
                mod_level == ModificationLevel.OTHER
                or (docmdp is not None and mod_level.value > docmdp.value)
            )
        elif coverage != SignatureCoverageLevel.ENTIRE_REVISION:
            # if the diff analysis didn't run, we can still do something
            # meaningful if coverage is not ENTIRE_REVISION:
            #  - if the signature covers the entire file, we're good.
            #  - if the coverage level is anything else, not so much
            docmdp_ok = coverage == SignatureCoverageLevel.ENTIRE_FILE

        status_kwargs = {
            'coverage': coverage,
            'docmdp_ok': docmdp_ok,
            'diff_result': diff_result,
        }
        return status_kwargs

    @property
    def seed_value_spec(self) -> Optional[SigSeedValueSpec]:
        try:
            sig_sv_dict = self.sig_field['/SV']
        except KeyError:
            return None
        return SigSeedValueSpec.from_pdf_object(sig_sv_dict)

    @property
    def docmdp_level(self) -> Optional[MDPPerm]:
        """
        :return:
            The document modification policy required by this signature or
            its Lock dictionary.

            .. warning::
                This does not take into account the DocMDP requirements of
                earlier signatures (if present).

                The specification forbids signing with a more lenient DocMDP
                than the one currently in force, so this should not happen
                in a compliant document.
                That being said, any potential violations will still invalidate
                the earlier signature with the stricter DocMDP policy.

        """
        if self._docmdp_queried:
            return self._docmdp
        docmdp = _extract_docmdp_for_sig(signature_obj=self.sig_object)

        if docmdp is None:
            try:
                lock_dict = self.sig_field['/Lock']
                docmdp = MDPPerm(lock_dict['/P'])
            except KeyError:
                pass
        self._docmdp = docmdp
        self._docmdp_queried = True
        return docmdp

    @property
    def fieldmdp(self) -> Optional[FieldMDPSpec]:
        """
        :return:
            Read the field locking policy of this signature, if applicable.
            See also :class:`~.pyhanko.sign.fields.FieldMDPSpec`.
        """
        # TODO as above, fall back to /Lock
        if self._fieldmdp_queried:
            return self._fieldmdp
        ref_dict = _extract_reference_dict(self.sig_object, '/FieldMDP')
        self._fieldmdp_queried = True
        if ref_dict is None:
            return None
        try:
            sp = FieldMDPSpec.from_pdf_object(ref_dict['/TransformParams'])
        except (ValueError, KeyError) as e:  # pragma: nocover
            raise SignatureValidationError(
                "Failed to read /FieldMDP settings"
            ) from e
        self._fieldmdp = sp
        return sp

    def compute_digest(self) -> bytes:
        """
        Compute the ``/ByteRange`` digest of this signature.
        The result will be cached.

        :return:
            The digest value.
        """
        if self.external_digest is not None:
            return self.external_digest

        self.total_len, digest = byte_range_digest(
            self.reader.stream,
            byte_range=self.byte_range,
            md_algorithm=self.external_md_algorithm,
        )
        self.external_digest = digest
        return digest

    def compute_tst_digest(self) -> Optional[bytes]:
        """
        Compute the digest of the signature needed to validate its timestamp
        token (if present).

        .. warning::
            This computation is only relevant for timestamp tokens embedded
            inside a regular signature.
            If the signature in question is a document timestamp (where the
            entire signature object is a timestamp token), this method
            does not apply.

        :return:
            The digest value, or ``None`` if there is no timestamp token.
        """

        if self.tst_signature_digest is not None:
            return self.tst_signature_digest
        self.tst_signature_digest = digest = compute_signature_tst_digest(
            self.signer_info
        )
        return digest

    def evaluate_signature_coverage(self) -> SignatureCoverageLevel:
        """
        Internal method used to evaluate the coverage level of a signature.

        :return:
            The coverage level of the signature.
        """

        xref_cache = self.reader.xrefs
        # for the coverage check, we're more strict with regards to the byte
        #  range
        stream = self.reader.stream

        # nonstandard byte range -> insta-fail
        if len(self.byte_range) != 4 or self.byte_range[0] != 0:
            return SignatureCoverageLevel.UNCLEAR

        _, len1, start2, len2 = self.byte_range

        # first check: check if the signature covers the entire file.
        #  (from a cryptographic point of view)
        # In this case, there are no changes at all, so we're good.

        # compute file size
        stream.seek(0, os.SEEK_END)
        # the * 2 is because of the ASCII hex encoding, and the + 2
        # is the wrapping <>
        embedded_sig_content = len(self.pkcs7_content) * 2 + 2
        signed_zone_len = len1 + len2 + embedded_sig_content
        file_covered = stream.tell() == signed_zone_len
        if file_covered:
            return SignatureCoverageLevel.ENTIRE_FILE

        # Now we're in the mixed case: the byte range is a standard one
        #  starting at the beginning of the document, but it doesn't go all
        #  the way to the end of the file. This can be for legitimate reasons,
        #  not all of which we can evaluate right now.

        # First, check if the signature is a contiguous one.
        # In other words, we need to check if the interruption in the byte
        # range is "fully explained" by the signature content.
        contiguous = start2 == len1 + embedded_sig_content
        if not contiguous:
            return SignatureCoverageLevel.UNCLEAR

        # next, we verify that the revision this signature belongs to
        #  is completely covered. This requires a few separate checks.
        # (1) Verify that the xref container (table or stream) is covered
        # (2) Verify the presence of the EOF and startxref markers at the
        #     end of the signed region, and compare them with the values
        #     in the xref cache to make sure we are reading the right revision.

        # Check (2) first, since it's the quickest
        stream.seek(signed_zone_len)
        signed_rev = self.signed_revision
        try:
            startxref = process_data_at_eof(stream)
            expected = xref_cache.get_startxref_for_revision(signed_rev)
            if startxref != expected:
                return SignatureCoverageLevel.CONTIGUOUS_BLOCK_FROM_START
        except misc.PdfReadError:
            return SignatureCoverageLevel.CONTIGUOUS_BLOCK_FROM_START

        # ... then check (1) for all revisions up to and including
        # signed_revision
        for revision in range(signed_rev + 1):
            xref_meta = xref_cache.get_xref_container_info(revision)
            if xref_meta.end_location > signed_zone_len:
                return SignatureCoverageLevel.CONTIGUOUS_BLOCK_FROM_START

        return SignatureCoverageLevel.ENTIRE_REVISION

    def _enforce_hybrid_xref_policy(self):
        reader = self.reader
        if reader.strict and reader.xrefs.hybrid_xrefs_present:
            raise SignatureValidationError(
                "Settings do not permit validation of signatures in "
                "hybrid-reference files."
            )

    def evaluate_modifications(
        self, diff_policy: DiffPolicy
    ) -> Union[DiffResult, SuspiciousModification]:
        """
        Internal method used to evaluate the modification level of a signature.
        """

        if self.coverage < SignatureCoverageLevel.ENTIRE_REVISION:
            return SuspiciousModification(
                'Nonstandard signature coverage level'
            )
        elif self.coverage == SignatureCoverageLevel.ENTIRE_FILE:
            return DiffResult(ModificationLevel.NONE, set())

        return diff_policy.review_file(
            self.reader,
            self.signed_revision,
            field_mdp_spec=self.fieldmdp,
            doc_mdp=self.docmdp_level,
        )


DocMDPInfo = namedtuple('DocMDPInfo', ['permission', 'author_sig'])
"""
Encodes certification information for a signed document, consisting of a
reference to the author signature, together with the associated DocMDP policy.
"""


def read_certification_data(reader: PdfFileReader) -> Optional[DocMDPInfo]:
    """
    Read the certification information for a PDF document, if present.

    :param reader:
        Reader representing the input document.
    :return:
        A :class:`.DocMDPInfo` object containing the relevant data, or ``None``.
    """
    try:
        certification_sig = reader.root['/Perms']['/DocMDP']
    except KeyError:
        return None

    perm = _extract_docmdp_for_sig(certification_sig)
    return DocMDPInfo(perm, certification_sig)


def _validate_sv_constraints(
    emb_sig: EmbeddedPdfSignature, validation_path, timestamp_found
):
    sv_spec = emb_sig.seed_value_spec
    if sv_spec is None:
        return
    signing_cert = emb_sig.signer_cert
    if sv_spec.cert is not None:
        try:
            sv_spec.cert.satisfied_by(signing_cert, validation_path)
        except UnacceptableSignerError as e:
            raise SigSeedValueValidationError(e) from e

    if not timestamp_found and sv_spec.timestamp_required:
        raise SigSeedValueValidationError(
            "The seed value dictionary requires a trusted timestamp, but "
            "none was found, or the timestamp did not validate."
        )

    sig_obj = emb_sig.sig_object

    if sv_spec.seed_signature_type is not None:
        sv_certify = sv_spec.seed_signature_type.certification_signature()
        try:
            perms: generic.DictionaryObject = emb_sig.reader.root['/Perms']
            cert_sig_ref = perms.get_value_as_reference('/DocMDP')
            was_certified = cert_sig_ref == sig_obj.container_ref
        except (KeyError, generic.IndirectObjectExpected, AttributeError):
            was_certified = False
        if sv_certify != was_certified:

            def _type(certify):
                return 'a certification' if certify else 'an approval'

            raise SigSeedValueValidationError(
                "The seed value dictionary's /MDP entry specifies that "
                f"this field should contain {_type(sv_certify)} "
                f"signature, but {_type(was_certified)} "
                "appears to have been used."
            )
        if sv_certify:
            sv_mdp_perm = sv_spec.seed_signature_type.mdp_perm
            doc_mdp = emb_sig.docmdp_level
            if sv_mdp_perm != doc_mdp:
                raise SigSeedValueValidationError(
                    "The seed value dictionary specified that this "
                    "certification signature should use the MDP policy "
                    f"'{sv_mdp_perm}', but '{doc_mdp}' was "
                    "used in the signature."
                )

    flags = sv_spec.flags
    if not flags:
        return

    selected_sf_str = sig_obj['/SubFilter']
    selected_sf = SigSeedSubFilter(selected_sf_str)
    if (flags & SigSeedValFlags.SUBFILTER) and sv_spec.subfilters is not None:
        # empty array = no supported subfilters
        if not sv_spec.subfilters:
            raise NotImplementedError(
                "The signature encodings mandated by the seed value "
                "dictionary are not supported."
            )
        # standard mandates that we take the first available subfilter
        mandated_sf: SigSeedSubFilter = sv_spec.subfilters[0]
        if selected_sf is not None and mandated_sf != selected_sf:
            raise SigSeedValueValidationError(
                "The seed value dictionary mandates subfilter '%s', "
                "but '%s' was used in the signature."
                % (mandated_sf.value, selected_sf.value)
            )

    if (
        flags & SigSeedValFlags.APPEARANCE_FILTER
    ) and sv_spec.appearance is not None:
        logger.warning(
            "The signature's seed value dictionary specifies the "
            "/AppearanceFilter entry as mandatory, but this constraint "
            "is impossible to validate."
        )

    if (
        flags & SigSeedValFlags.LEGAL_ATTESTATION
    ) and sv_spec.legal_attestations is not None:
        raise NotImplementedError(
            "pyHanko does not support legal attestations, but the seed value "
            "dictionary mandates that they be restricted to a specific subset."
        )

    if (
        flags & SigSeedValFlags.LOCK_DOCUMENT
    ) and sv_spec.lock_document is not None:
        doc_mdp = emb_sig.docmdp_level
        if (
            sv_spec.lock_document == SeedLockDocument.LOCK
            and doc_mdp != MDPPerm.NO_CHANGES
        ):
            raise SigSeedValueValidationError(
                "Document must be locked, but some changes are still allowed."
            )
        if (
            sv_spec.lock_document == SeedLockDocument.DO_NOT_LOCK
            and doc_mdp == MDPPerm.NO_CHANGES
        ):
            raise SigSeedValueValidationError(
                "Document must not be locked, but the DocMDP level is set to "
                "NO_CHANGES."
            )
        # value 'auto' is OK.

    signer_info = emb_sig.signer_info
    if (
        flags & SigSeedValFlags.ADD_REV_INFO
    ) and sv_spec.add_rev_info is not None:
        from pyhanko.sign.validation.ltv import retrieve_adobe_revocation_info

        try:
            retrieve_adobe_revocation_info(signer_info)
            revinfo_found = True
        except ValidationInfoReadingError:
            revinfo_found = False

        if sv_spec.add_rev_info != revinfo_found:
            raise SigSeedValueValidationError(
                "The seed value dict mandates that revocation info %sbe "
                "added, but it was %sfound in the signature."
                % (
                    "" if sv_spec.add_rev_info else "not ",
                    "" if revinfo_found else "not ",
                )
            )
        if (
            sv_spec.add_rev_info
            and selected_sf != SigSeedSubFilter.ADOBE_PKCS7_DETACHED
        ):
            raise SigSeedValueValidationError(
                "The seed value dict mandates that Adobe-style revocation "
                "info be added; this requires subfilter '%s'"
                % (SigSeedSubFilter.ADOBE_PKCS7_DETACHED.value)
            )

    if (
        flags & SigSeedValFlags.DIGEST_METHOD
    ) and sv_spec.digest_methods is not None:
        selected_md = emb_sig.md_algorithm.lower()
        if selected_md not in sv_spec.digest_methods:
            raise SigSeedValueValidationError(
                "The selected message digest %s is not allowed by the "
                "seed value dictionary." % selected_md
            )

    if flags & SigSeedValFlags.REASONS:
        # standard says that omission of the /Reasons key amounts to
        #  a prohibition in this case
        reasons = sv_spec.reasons or []
        must_omit = not reasons or reasons == ["."]
        reason_given = sig_obj.get('/Reason')
        if must_omit and reason_given is not None:
            raise SigSeedValueValidationError(
                "The seed value dictionary prohibits giving a reason "
                "for signing."
            )
        if not must_omit and reason_given not in reasons:
            raise SigSeedValueValidationError(
                "The reason for signing \"%s\" is not accepted by the "
                "seed value dictionary." % (reason_given,)
            )


def report_seed_value_validation(
    embedded_sig: EmbeddedPdfSignature,
    validation_path: ValidationPath,
    timestamp_found: bool,
):
    """
    Internal API function to enforce seed value constraints (if present)
    and report on the result(s).

    :param embedded_sig:
        The embedded signature.
    :param validation_path:
        The validation path for the signer's certificate.
    :param timestamp_found:
        Flag indicating whether a valid timestamp was found or not.
    :return:
        A ``status_kwargs`` dict.
    """
    sv_err: Optional[SigSeedValueValidationError]
    try:
        _validate_sv_constraints(
            embedded_sig, validation_path, timestamp_found=timestamp_found
        )
        sv_err = None
    except SigSeedValueValidationError as e:
        logger.warning("Error in seed value validation.", exc_info=e)
        sv_err = e
    return {
        'has_seed_values': embedded_sig.seed_value_spec is not None,
        'seed_value_constraint_error': sv_err,
    }


def _validate_subfilter(subfilter_str, permitted_subfilters, err_msg):
    try:
        from pyhanko.sign.fields import SigSeedSubFilter

        subfilter_ok = SigSeedSubFilter(subfilter_str) in permitted_subfilters
    except ValueError:
        subfilter_ok = False

    if not subfilter_ok:
        raise SignatureValidationError(err_msg % subfilter_str)


async def async_validate_pdf_signature(
    embedded_sig: EmbeddedPdfSignature,
    signer_validation_context: Optional[ValidationContext] = None,
    ts_validation_context: Optional[ValidationContext] = None,
    ac_validation_context: Optional[ValidationContext] = None,
    diff_policy: Optional[DiffPolicy] = None,
    key_usage_settings: Optional[KeyUsageConstraints] = None,
    skip_diff: bool = False,
    algorithm_policy: Optional[CMSAlgorithmUsagePolicy] = None,
) -> PdfSignatureStatus:
    """
    .. versionadded:: 0.9.0

    .. versionchanged: 0.11.0
        Added ``ac_validation_context`` param.


    Validate a PDF signature.

    :param embedded_sig:
        Embedded signature to evaluate.
    :param signer_validation_context:
        Validation context to use to validate the signature's chain of trust.
    :param ts_validation_context:
        Validation context to use to validate the timestamp's chain of trust
        (defaults to ``signer_validation_context``).
    :param ac_validation_context:
        Validation context to use to validate attribute certificates.
        If not supplied, no AC validation will be performed.

        .. note::
            :rfc:`5755` requires attribute authority trust roots to be specified
            explicitly; hence why there's no default.
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
    :param algorithm_policy:
        The algorithm usage policy for the signature validation.

        .. warning::
            This is distinct from the algorithm usage policy used for
            certificate validation, but the latter will be used as a fallback
            if this parameter is not specified.

            It is nonetheless recommended to align both policies unless
            there is a clear reason to do otherwise.
    :return:
        The status of the PDF signature in question.
    """

    sig_object = embedded_sig.sig_object
    if embedded_sig.sig_object_type != '/Sig':
        raise SignatureValidationError("Signature object type must be /Sig")

    # check whether the subfilter type is one we support
    subfilter_str = sig_object.get('/SubFilter', None)
    _validate_subfilter(
        subfilter_str,
        (SigSeedSubFilter.ADOBE_PKCS7_DETACHED, SigSeedSubFilter.PADES),
        "%s is not a recognized SubFilter type in signatures.",
    )

    if ts_validation_context is None:
        ts_validation_context = signer_validation_context

    embedded_sig.compute_integrity_info(
        diff_policy=diff_policy, skip_diff=skip_diff
    )
    status_kwargs = embedded_sig.summarise_integrity_info()

    ts_status_kwargs = await collect_timing_info(
        embedded_sig.signer_info,
        ts_validation_context,
        raw_digest=embedded_sig.compute_digest(),
    )
    status_kwargs.update(ts_status_kwargs)
    if 'signer_reported_dt' not in status_kwargs:
        # maybe the PDF signature dictionary declares /M
        signer_reported_dt = embedded_sig.self_reported_timestamp
        if signer_reported_dt is not None:
            status_kwargs['signer_reported_dt'] = signer_reported_dt

    key_usage_settings = PdfSignatureStatus.default_usage_constraints(
        key_usage_settings
    )
    status_kwargs = await cms_basic_validation(
        embedded_sig.signed_data,
        raw_digest=embedded_sig.external_digest,
        validation_context=signer_validation_context,
        status_kwargs=status_kwargs,
        key_usage_settings=key_usage_settings,
        algorithm_policy=algorithm_policy,
    )
    tst_validity = status_kwargs.get('timestamp_validity', None)
    timestamp_found = (
        tst_validity is not None and tst_validity.valid and tst_validity.trusted
    )
    sv_update = report_seed_value_validation(
        embedded_sig, status_kwargs['validation_path'], timestamp_found
    )
    status_kwargs.update(sv_update)
    if ac_validation_context is not None:
        ac_validation_context.certificate_registry.register_multiple(
            embedded_sig.other_embedded_certs
        )
    status_kwargs.update(
        await collect_signer_attr_status(
            sd_attr_certificates=embedded_sig.embedded_attr_certs,
            signer_cert=embedded_sig.signer_cert,
            validation_context=ac_validation_context,
            sd_signed_attrs=embedded_sig.signer_info['signed_attrs'],
        )
    )
    return PdfSignatureStatus(**status_kwargs)


async def async_validate_pdf_timestamp(
    embedded_sig: EmbeddedPdfSignature,
    validation_context: Optional[ValidationContext] = None,
    diff_policy: Optional[DiffPolicy] = None,
    skip_diff: bool = False,
) -> DocumentTimestampStatus:
    """
    .. versionadded:: 0.9.0

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

    if embedded_sig.sig_object_type != '/DocTimeStamp':
        raise SignatureValidationError(
            "Signature object type must be /DocTimeStamp"
        )

    # check whether the subfilter type is one we support
    subfilter_str = embedded_sig.sig_object.get('/SubFilter', None)
    _validate_subfilter(
        subfilter_str,
        (SigSeedSubFilter.ETSI_RFC3161,),
        "%s is not a recognized SubFilter type for timestamps.",
    )

    embedded_sig.compute_integrity_info(
        diff_policy=diff_policy, skip_diff=skip_diff
    )

    status_kwargs = await validate_tst_signed_data(
        embedded_sig.signed_data,
        validation_context,
        embedded_sig.compute_digest(),
    )

    status_kwargs['coverage'] = embedded_sig.coverage
    status_kwargs['diff_result'] = embedded_sig.diff_result
    return DocumentTimestampStatus(**status_kwargs)
