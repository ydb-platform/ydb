from asn1crypto.algos import KdfAlgorithmId
from asn1crypto.cms import (
    CMSAttribute,
    CMSAttributeType,
    ContentInfo,
    ContentType,
    EncapsulatedContentInfo,
    SetOfContentInfo,
)
from asn1crypto.core import Integer, OctetString, Sequence


class PdfMacIntegrityInfo(Sequence):
    _fields = [
        ('version', Integer),
        ('data_digest', OctetString),
        ('signature_digest', OctetString, {'implicit': 0, 'optional': True}),
    ]


def asn1crypto_setup():
    # This is the content type used for the MAC'd data in a PDF MAC token
    ContentType._map['1.0.32004.1.0'] = 'pdf_mac_integrity_info'
    if ContentType._reverse_map is not None:
        ContentType._reverse_map['pdf_mac_integrity_info'] = (
            '1.0.32004.1.0'  # pragma: nocover
        )
    EncapsulatedContentInfo._oid_specs['pdf_mac_integrity_info'] = (
        ContentInfo._oid_specs['pdf_mac_integrity_info']
    ) = PdfMacIntegrityInfo

    # This OID represents the key derivation function defined in ISO 32004
    # to derive the MAC key encryption key from the PDF's file encryption key
    #  (+ other data)
    KdfAlgorithmId._map['1.0.32004.1.1'] = 'pdf_mac_wrap_kdf'
    if KdfAlgorithmId._reverse_map is not None:
        KdfAlgorithmId._reverse_map['pdf_mac_wrap_kdf'] = (
            '1.0.32004.1.1'  # pragma: nocover
        )

    # This OID represents the attribute used when attaching PDF MAC tokens
    # to signatures.
    CMSAttributeType._map['1.0.32004.1.2'] = 'pdf_mac_data'
    if CMSAttributeType._reverse_map is not None:
        CMSAttributeType._reverse_map['1.0.32004.1.2'] = (
            'pdf_mac_data'  # pragma: nocover
        )
    CMSAttribute._oid_specs['pdf_mac_data'] = SetOfContentInfo


asn1crypto_setup()
