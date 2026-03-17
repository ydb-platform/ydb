"""
ETSI TS 119 102-2 reporting functionality.

.. warning::
    This feature is incubating and subject to API changes.
"""

from typing import Any, Dict, Optional, cast

from asn1crypto import tsp
from cryptography.hazmat.primitives import hashes
from pyhanko_certvalidator.ltv.poe import ValidationObject, ValidationObjectType
from xsdata.models.datatype import XmlDateTime

from pyhanko.generated.etsi import ts_11910202, xades
from pyhanko.generated.w3c import xmldsig_core
from pyhanko.sign.ades import cades_asn1
from pyhanko.sign.ades.report import AdESStatus
from pyhanko.sign.general import (
    NonexistentAttributeError,
    find_cms_attribute,
    find_unique_cms_attribute,
    get_pyca_cryptography_hash,
)
from pyhanko.sign.validation.ades import (
    AdESBasicValidationResult,
    AdESLTAValidationResult,
    AdESWithTimeValidationResult,
    derive_validation_object_binary_data,
    derive_validation_object_identifier,
)
from pyhanko.sign.validation.generic_cms import get_signing_cert_attr
from pyhanko.sign.validation.pdf_embedded import EmbeddedPdfSignature
from pyhanko.sign.validation.status import PdfSignatureStatus

__all__ = ['generate_report']

DIGEST_ALGO_URIS = {
    'sha1': 'http://www.w3.org/2000/09/xmldsig#sha1',
    'sha256': 'http://www.w3.org/2001/04/xmlenc#sha256',
    'sha224': 'http://www.w3.org/2001/04/xmldsig-more#sha224',
    'sha384': 'http://www.w3.org/2001/04/xmldsig-more#sha384',
    'sha512': 'http://www.w3.org/2001/04/xmlenc#sha512',
}

NAMESPACES = {
    'vr': 'http://uri.etsi.org/19102/v1.2.1#',
    'XAdES': 'http://uri.etsi.org/01903/v1.3.2#',
    'ds': 'http://www.w3.org/2000/09/xmldsig#',
    'xs': 'http://www.w3.org/2001/XMLSchema',
}


def _digest_algo_uri(algo: str):
    try:
        return DIGEST_ALGO_URIS[algo]
    except KeyError:
        raise NotImplementedError(
            f"No XML signature syntax available for digest algo '{algo}'"
        )


def _summarise_attrs(
    embedded_sig: EmbeddedPdfSignature, api_status: PdfSignatureStatus
):
    # TODO refactor this to use a provider pattern so it can be
    #  more easily generalised to CAdES or even XAdES

    signed_attrs = embedded_sig.signer_info['signed_attrs']

    # signing_time (SASigningTimeType)
    kwargs: Dict[str, Any] = {}
    claimed_time = embedded_sig.self_reported_timestamp or (
        api_status.timestamp_validity.timestamp
        if api_status.timestamp_validity
        else None
    )
    if claimed_time:
        kwargs['signing_time'] = ts_11910202.SASigningTimeType(
            signed=True,
            time=XmlDateTime.from_datetime(claimed_time),
        )
    # signing_certificate (SACertIDListType)
    signing_cert_attr = get_signing_cert_attr(signed_attrs)
    if signing_cert_attr is not None:
        cert_ids_xml = []
        for cert_id in signing_cert_attr['certs']:
            if isinstance(cert_id, tsp.ESSCertID):
                hash_algo = 'sha1'
            else:
                hash_algo = cert_id['hash_algorithm']['algorithm'].native
            cert_ids_xml.append(
                ts_11910202.SACertIDType(
                    digest_method=xmldsig_core.DigestMethod(
                        _digest_algo_uri(hash_algo),
                    ),
                    digest_value=xmldsig_core.DigestValue(
                        cert_id['cert_hash'].native
                    ),
                    x509_issuer_serial=(
                        cert_id['issuer_serial'].dump()
                        if cert_id['issuer_serial']
                        else None
                    ),
                )
            )
        kwargs['signing_certificate'] = ts_11910202.SACertIDListType(
            signed=True, cert_id=tuple(cert_ids_xml)
        )
    # data_object_format (SADataObjectFormatType) -> not applicable
    # commitment_type_indication (SACommitmentTypeIndicationType)
    try:
        commitment_type: cades_asn1.CommitmentTypeIndication = (
            find_unique_cms_attribute(
                signed_attrs, 'commitment_type_indication'
            )
        )
        oid = commitment_type['commitment_type_id']
        kwargs['commitment_type_indication'] = (
            ts_11910202.SACommitmentTypeIndicationType(
                signed=True, commitment_type_identifier=f'urn:oid:{oid.dotted}'
            )
        )
    except NonexistentAttributeError:
        pass
    # all_data_objects_time_stamp (SATimestampType)
    if api_status.content_timestamp_validity:
        kwargs['all_data_objects_time_stamp'] = ts_11910202.SATimestampType(
            signed=True,
            time_stamp_value=XmlDateTime.from_datetime(
                api_status.content_timestamp_validity.timestamp
            ),
        )
    # individual_data_objects_time_stamp (SATimestampType) -> not applicable
    # sig_policy_identifier (SASigPolicyIdentifierType)
    try:
        sig_policy_ident: cades_asn1.SignaturePolicyIdentifier = (
            find_cms_attribute(signed_attrs, 'signature_policy_identifier')[0]
        )
        actual_policy_ident = sig_policy_ident.chosen
        # we don't support implicit policies (or at least not now)
        if isinstance(actual_policy_ident, cades_asn1.SignaturePolicyId):
            oid = actual_policy_ident['sig_policy_id']
            ident_xml = ts_11910202.SASigPolicyIdentifierType(
                signed=True, sig_policy_id=f'urn:oid:{oid.dotted}'
            )
            kwargs['sig_policy_identifier'] = ident_xml
    except NonexistentAttributeError:
        pass

    # signature_production_place (SASignatureProductionPlaceType)
    if '/Location' in embedded_sig.sig_object:
        kwargs['signature_production_place'] = (
            ts_11910202.SASignatureProductionPlaceType(
                signed=True,
                address_string=(str(embedded_sig.sig_object['/Location']),),
            )
        )
    # signer_role (SASignerRoleType)
    if api_status.cades_signer_attrs:
        cades_signer_attrs = api_status.cades_signer_attrs
        roles = []
        for claimed_attr in cades_signer_attrs.claimed_attrs:
            role_type = ts_11910202.SAOneSignerRoleTypeEndorsementType.CLAIMED
            stringified = (
                f"{claimed_attr.attr_type.native}: "
                f"{'; '.join(str(v.native) for v in claimed_attr.attr_values)}"
            )
            roles.append(
                ts_11910202.SAOneSignerRoleType(
                    endorsement_type=role_type, role=stringified
                )
            )
        for cert_attr in cades_signer_attrs.certified_attrs or ():
            role_type = ts_11910202.SAOneSignerRoleTypeEndorsementType.CERTIFIED
            # TODO include provenance info in the string representation?
            stringified = (
                f"{cert_attr.attr_type.native} "
                f"{'; '.join(str(v.native) for v in cert_attr.attr_values)}"
            )
            roles.append(
                ts_11910202.SAOneSignerRoleType(
                    endorsement_type=role_type, role=stringified
                )
            )
        kwargs['signer_role'] = ts_11910202.SASignerRoleType(
            signed=True, role_details=tuple(roles)
        )

    # counter_signature (SACounterSignatureType)
    #  -> not reasonably implementable in PDF signatures
    #     and banned in PAdES, skip
    # signature_time_stamp (SATimestampType)
    if api_status.timestamp_validity:
        kwargs['signature_time_stamp'] = ts_11910202.SATimestampType(
            signed=False,
            time_stamp_value=XmlDateTime.from_datetime(
                api_status.timestamp_validity.timestamp
            ),
        )
    # complete_certificate_refs (SACertIDListType) -> not in PAdES
    # complete_revocation_refs (SARevIDListType) -> not in PAdES
    # attribute_certificate_refs (SACertIDListType) -> not in PAdES
    # attribute_revocation_refs (SARevIDListType) -> not in PAdES
    # sig_and_refs_time_stamp (SATimestampType) -> not in PAdES
    # refs_only_time_stamp (SATimestampType) -> not in PAdES
    # certificate_values (AttributeBaseType) -> not in PAdES
    # revocation_values (AttributeBaseType) -> not in PAdES
    # attr_authorities_cert_values (AttributeBaseType) -> not in PAdES
    # attribute_revocation_values (AttributeBaseType) -> not in PAdES
    # time_stamp_validation_data (AttributeBaseType) -> XAdES-exclusive
    # archive_time_stamp (SATimestampType) -> not in PAdES
    # renewed_digests: tuple(int, ...) -> XAdES-exclusive

    # message_digest (SAMessageDigestType)
    # for invalid sigs, this is worth reporting as specified
    try:
        message_digest = find_unique_cms_attribute(
            signed_attrs, 'message_digest'
        )
        kwargs['message_digest'] = ts_11910202.SAMessageDigestType(
            signed=True, digest=message_digest.native
        )
    except NonexistentAttributeError:
        pass
    # dss (SADSSType)
    # TODO (emitting validation objects)
    # vri (SAVRIType)
    # TODO (emitting validation objects)
    # doc_time_stamp (SATimestampType)
    # TODO (emitting validation objects)
    # reason (SAReasonType)
    if '/Reason' in embedded_sig.sig_object:
        kwargs['reason'] = ts_11910202.SAReasonType(
            signed=True, reason_element=str(embedded_sig.sig_object['/Reason'])
        )
    # name (SANameType)
    if '/Name' in embedded_sig.sig_object:
        kwargs['name'] = ts_11910202.SANameType(
            signed=True,
            name_element=str(embedded_sig.sig_object['/Name']),
        )
    # contact_info (SAContactInfoType)
    if '/ContactInfo' in embedded_sig.sig_object:
        kwargs['contact_info'] = ts_11910202.SAContactInfoType(
            signed=True,
            contact_info_element=str(embedded_sig.sig_object['/ContactInfo']),
        )
    # sub_filter (SASubFilterType)
    if '/SubFilter' in embedded_sig.sig_object:
        kwargs['sub_filter'] = ts_11910202.SASubFilterType(
            signed=True,
            sub_filter_element=str(embedded_sig.sig_object['/SubFilter'])[1:],
        )
    # byte_range: (int, int, int, int)
    kwargs['byte_range'] = tuple(
        int(x) for x in embedded_sig.sig_object['/ByteRange']
    )
    # filter (SAFilterType)
    if '/Filter' in embedded_sig.sig_object:
        kwargs['filter'] = ts_11910202.SAFilterType(
            filter=str(embedded_sig.sig_object['/Filter'])[1:],
        )
    return ts_11910202.SignatureAttributesType(**kwargs)


def _generate_report(
    embedded_sig: EmbeddedPdfSignature, status: AdESBasicValidationResult
) -> ts_11910202.SignatureValidationReportType:
    api_status: PdfSignatureStatus = cast(PdfSignatureStatus, status.api_status)
    # this is meaningless for EdDSA signatures, but the entry is mandatory, so...
    md_spec = get_pyca_cryptography_hash(api_status.md_algorithm)
    md = hashes.Hash(md_spec)
    md.update(embedded_sig.signer_info['signed_attrs'].dump())
    dtbsr_digest = md.finalize()
    dtbsr_digest_info = xades.DigestAlgAndValueType(
        digest_method=xmldsig_core.DigestMethod(
            _digest_algo_uri(api_status.md_algorithm)
        ),
        digest_value=xmldsig_core.DigestValue(dtbsr_digest),
    )
    sig_id = ts_11910202.SignatureIdentifierType(
        signature_value=xmldsig_core.SignatureValue(
            embedded_sig.signer_info['signature'].native
        ),
        digest_alg_and_value=dtbsr_digest_info,
        hash_only=False,
        doc_hash_only=False,
    )
    if isinstance(status, AdESLTAValidationResult):
        process = 'LTA'
    elif isinstance(status, AdESWithTimeValidationResult):
        process = 'LTVM'
    else:
        process = 'Basic'
    ades_main_indic = {
        AdESStatus.PASSED: 'urn:etsi:019102:mainindication:total-passed',
        AdESStatus.FAILED: 'urn:etsi:019102:mainindication:total-failed',
        AdESStatus.INDETERMINATE: 'urn:etsi:019102:mainindication:indeterminate',
    }[status.ades_subindic.status]
    validation_time = api_status.validation_time
    assert validation_time is not None
    best_sig_time: Optional[ts_11910202.POEType] = None
    if isinstance(status, AdESWithTimeValidationResult):
        best_sig_time = ts_11910202.POEType(
            poetime=XmlDateTime.from_datetime(status.best_signature_time),
            # TODO extend POE semantics to preserve this info,
            #  for now we simply claim it was derived during validation
            type_of_proof='urn:etsi:019102:poetype:validation',
        )
    signer_cert_vo = ValidationObject(
        object_type=ValidationObjectType.CERTIFICATE,
        value=api_status.signing_cert,
    )
    single_report = ts_11910202.SignatureValidationReportType(
        signature_identifier=sig_id,
        # TODO validation constraints eval report
        validation_time_info=ts_11910202.ValidationTimeInfoType(
            validation_time=XmlDateTime.from_datetime(validation_time),
            best_signature_time=best_sig_time,
        ),
        signers_document=ts_11910202.SignersDocumentType(
            digest_alg_and_value=xades.DigestAlgAndValueType(
                digest_method=xmldsig_core.DigestMethod(
                    _digest_algo_uri(api_status.md_algorithm)
                ),
                digest_value=xmldsig_core.DigestValue(
                    embedded_sig.compute_digest()
                ),
            ),
        ),
        signature_attributes=_summarise_attrs(embedded_sig, api_status),
        signer_information=ts_11910202.SignerInformationType(
            signer_certificate=ts_11910202.VOReferenceType(
                voreference=(
                    f"{derive_validation_object_identifier(signer_cert_vo)}",
                ),
            ),
        ),
        # TODO quality -> needs Qualification Algorithm support
        signature_validation_process=ts_11910202.SignatureValidationProcessType(
            signature_validation_process_id=(
                f'urn:etsi:019102:validationprocess:{process}'
            )
        ),
        signature_validation_status=ts_11910202.ValidationStatusType(
            main_indication=ades_main_indic,
            sub_indication=(
                f'urn:etsi:019102:subindication:{status.ades_subindic.standard_name}',
            ),
        ),
    )
    return single_report


def _package_validation_object(vo: ValidationObject):
    bin_data = derive_validation_object_binary_data(vo)
    # TODO preserve POE info, sub-validation reports (mainly for timestamps)
    return ts_11910202.ValidationObjectType(
        id=derive_validation_object_identifier(vo),
        object_type=vo.object_type.urn(),
        validation_object_representation=(
            ts_11910202.ValidationObjectRepresentationType(base64=bin_data)
            if bin_data
            else None
        ),
    )


def generate_report(
    embedded_sig: EmbeddedPdfSignature, status: AdESBasicValidationResult
) -> str:
    """
    Generate signature validation report in XML format according to
    ETSI TS 119 102-2.

    :param embedded_sig:
        PDF signature to report on.
    :param status:
        AdES validation result to turn into a report.
    :return:
        A string representation of the validation report.
    """
    report = ts_11910202.ValidationReport(
        signature_validation_report=(_generate_report(embedded_sig, status),),
        signature_validation_objects=ts_11910202.ValidationObjectListType(
            tuple(
                _package_validation_object(vo)
                for vo in status.validation_objects
            )
        ),
    )
    from xsdata.formats.dataclass.serializers import XmlSerializer
    from xsdata.formats.dataclass.serializers.config import SerializerConfig

    config = SerializerConfig(indent="  ")
    ser = XmlSerializer(config=config).render(report, ns_map=NAMESPACES)
    return ser
