import os
import secrets
from hmac import compare_digest
from typing import Any, Dict, FrozenSet, Optional, Tuple, Type

from asn1crypto import algos, cms, core
from asn1crypto.core import VOID
from cryptography.hazmat.primitives import hashes, hmac, keywrap
from cryptography.hazmat.primitives.kdf import hkdf

from pyhanko.pdf_utils import generic, misc

from ...sign.general import (
    CMSStructuralError,
    MultivaluedAttributeError,
    NonexistentAttributeError,
    ValueErrorWithMessage,
    byte_range_digest,
    find_unique_cms_attribute,
    get_pyca_cryptography_hash,
    simple_cms_attribute,
)
from ...sign.signers.pdf_byterange import (
    PdfByteRangeDigest,
    PreparedByteRangeDigest,
)
from ...sign.validation.errors import (
    CMSAlgorithmProtectionError,
    DisallowedAlgorithmError,
)
from ...sign.validation.generic_cms import validate_algorithm_protection
from ...sign.validation.pdf_embedded import extract_contents
from ..extensions import DeveloperExtension, DevExtensionMultivalued
from ..generic import pdf_name
from ..reader import PdfFileReader
from ..writer import BasePdfFileWriter
from ._iso32004_asn1 import PdfMacIntegrityInfo

__all__ = [
    'PdfMacTokenHandler',
    'validate_pdf_mac',
    'add_standalone_mac',
    'ISO32004',
    'ALLOWED_MD_ALGS',
]


ISO32004 = DeveloperExtension(
    prefix_name=generic.pdf_name('/ISO_'),
    base_version=generic.pdf_name('/2.0'),
    extension_level=32004,
    extension_revision=':2024',
    url='https://www.iso.org/standard/45877.html',
    compare_by_level=False,
    multivalued=DevExtensionMultivalued.ALWAYS,
)


ALLOWED_MD_ALGS = frozenset(
    [
        'sha256',
        'sha3_256',
        'sha384',
        'sha3_384',
        'sha512',
        'sha3_512',
    ]
)
"""
Specifies the list of permissible message digest algorithms in PDF MAC tokens.
"""


class PdfMacValidationError(ValueErrorWithMessage):
    pass


class PdfMacTokenHandler:
    """
    Internal utility class to create and validate PDF MAC tokens.

    .. warning::
        This is a class to simplify local overrides for creating test documents
        with various defects.

        Instances of this class should never be created or manipulated directly
        during regular operation.
    """

    mac_algo_ident = cms.HmacAlgorithm({'algorithm': 'sha256'})

    def __init__(self, *, mac_kek, md_algorithm):
        self.mac_kek = mac_kek
        self.md_algorithm = md_algorithm

    @classmethod
    def from_key_mat(
        cls, file_encryption_key: bytes, kdf_salt: bytes, md_algorithm: str
    ):
        mac_kek = cls._derive_mac_kek(file_encryption_key, kdf_salt)
        return cls(mac_kek=mac_kek, md_algorithm=md_algorithm)

    @classmethod
    def for_validation(
        cls,
        file_encryption_key: bytes,
        kdf_salt: bytes,
        auth_data: cms.AuthenticatedData,
        allowed_mds: FrozenSet[str] = ALLOWED_MD_ALGS,
    ):
        # First, check the declared MAC and digesting algorithms used
        mac_algo_obj: algos.HmacAlgorithm = auth_data['mac_algorithm']
        mac_algo = mac_algo_obj['algorithm'].native

        # remember: this is actually HMAC-SHA256 in asn1crypto
        if mac_algo != 'sha256':
            raise NotImplementedError(
                "Only HMAC-SHA256 is currently supported for PDF MAC tokens"
            )

        digest_algo_obj: algos.DigestAlgorithm = auth_data['digest_algorithm']
        md_algo = digest_algo_obj['algorithm'].native
        if md_algo not in allowed_mds:
            raise DisallowedAlgorithmError(
                md_algo,
                oid_type=algos.DigestAlgorithmId,
                # TODO extend algo policy to deal with this.
                permanent=True,
            )
        return cls.from_key_mat(
            file_encryption_key=file_encryption_key,
            kdf_salt=kdf_salt,
            md_algorithm=md_algo,
        )

    def determine_token_size(self, include_signature_digest: bool) -> int:
        dummy_hash = hashes.Hash(
            get_pyca_cryptography_hash(self.md_algorithm)
        ).finalize()

        dummy_token = self.build_pdfmac_token(
            document_digest=dummy_hash,
            signature_digest=dummy_hash if include_signature_digest else None,
            dry_run=True,
        )

        return len(dummy_token.dump())

    @classmethod
    def _derive_mac_kek(
        cls, file_encryption_key: bytes, kdf_salt: bytes
    ) -> bytes:
        # Derive the key encryption key
        kdf = hkdf.HKDF(
            algorithm=hashes.SHA256(),
            length=32,
            salt=kdf_salt,
            info=b'PDFMAC',
        )
        return kdf.derive(file_encryption_key)

    def _format_auth_attrs(self, message_digest: bytes) -> cms.CMSAttributes:
        algo_protection = cms.CMSAlgorithmProtection(
            {
                'mac_algorithm': self.mac_algo_ident,
                'digest_algorithm': cms.DigestAlgorithm(
                    {'algorithm': self.md_algorithm}
                ),
            }
        )
        return cms.CMSAttributes(
            [
                simple_cms_attribute('content_type', 'pdf_mac_integrity_info'),
                simple_cms_attribute('message_digest', message_digest),
                simple_cms_attribute(
                    'cms_algorithm_protection', algo_protection
                ),
            ]
        )

    def _get_mac_keying_info(
        self, dry_run: bool
    ) -> Tuple[bytes, cms.RecipientInfo]:
        # Generate the actual MAC key, encrypt it and package it up in a PWRI
        if dry_run:
            mac_key = bytes(32)
        else:
            mac_key = secrets.token_bytes(32)
        encrypted_key = keywrap.aes_key_wrap(
            wrapping_key=self.mac_kek, key_to_wrap=mac_key
        )

        pwri = cms.PasswordRecipientInfo(
            {
                'version': 0,
                'encrypted_key': encrypted_key,
                'key_derivation_algorithm': algos.KdfAlgorithm(
                    {
                        'algorithm': 'pdf_mac_wrap_kdf',
                    }
                ),
                'key_encryption_algorithm': cms.KeyEncryptionAlgorithm(
                    {'algorithm': 'aes256_wrap'}
                ),
            }
        )

        return mac_key, cms.RecipientInfo({'pwri': pwri})

    def _format_message(
        self, document_digest: bytes, signature_digest: Optional[bytes]
    ) -> Tuple[bytes, bytes]:
        message_kwargs: Dict[str, Any] = {'data_digest': document_digest}
        if signature_digest is not None:
            message_kwargs['signature_digest'] = signature_digest
        message_kwargs['version'] = 0

        message = PdfMacIntegrityInfo(message_kwargs)
        message_bytes = message.dump()

        md_spec = get_pyca_cryptography_hash(self.md_algorithm)
        md_fun = hashes.Hash(md_spec)
        md_fun.update(message_bytes)
        message_digest = md_fun.finalize()

        return message_bytes, message_digest

    def _format_auth_data(
        self,
        recipient_info: cms.RecipientInfo,
        message_bytes: bytes,
        auth_attrs: cms.CMSAttributes,
        mac: bytes,
    ) -> cms.AuthenticatedData:
        return cms.AuthenticatedData(
            {
                'version': 0,
                'recipient_infos': [recipient_info],
                'mac_algorithm': cms.HmacAlgorithm({'algorithm': 'sha256'}),
                'digest_algorithm': cms.DigestAlgorithm(
                    {'algorithm': self.md_algorithm}
                ),
                'encap_content_info': cms.EncapsulatedContentInfo(
                    {
                        'content_type': 'pdf_mac_integrity_info',
                        'content': core.ParsableOctetString(message_bytes),
                    }
                ),
                'auth_attrs': auth_attrs,
                'mac': mac,
            }
        )

    def compute_mac(self, mac_key: bytes, data_to_mac: bytes) -> bytes:
        hmac_fun = hmac.HMAC(key=mac_key, algorithm=hashes.SHA256())
        hmac_fun.update(data_to_mac)
        return hmac_fun.finalize()

    def build_pdfmac_token(
        self,
        *,
        document_digest: bytes,
        signature_digest: Optional[bytes],
        dry_run: bool = False,
    ) -> cms.ContentInfo:
        mac_key, ri = self._get_mac_keying_info(dry_run=dry_run)

        message_bytes, message_digest = self._format_message(
            document_digest, signature_digest
        )

        auth_attrs = self._format_auth_attrs(message_digest)
        mac = self.compute_mac(
            mac_key=mac_key, data_to_mac=auth_attrs.untag().dump()
        )
        authed_data = self._format_auth_data(
            recipient_info=ri,
            message_bytes=message_bytes,
            auth_attrs=auth_attrs,
            mac=mac,
        )
        return cms.ContentInfo(
            {'content_type': 'authenticated_data', 'content': authed_data}
        )

    def _retrieve_mac_key(self, recp_infos: cms.RecipientInfos) -> bytes:
        pwri = None
        try:
            (recp,) = recp_infos
            pwri = recp.chosen
        except ValueError:
            pass

        if not isinstance(pwri, cms.PasswordRecipientInfo):
            raise PdfMacValidationError(
                "PDF MAC requires exactly one recipientInfo, which must be "
                "of PasswordRecipientInfo type"
            )

        kdf = pwri['key_derivation_algorithm']
        if kdf is VOID or kdf['algorithm'].native != 'pdf_mac_wrap_kdf':
            raise PdfMacValidationError(
                "PDF MAC tokens must have their key derivation algorithm "
                "explicitly identified as pdfMacWrapKdf."
            )

        kea_obj = pwri['key_encryption_algorithm']['algorithm']
        if kea_obj.native != 'aes256_wrap':
            raise NotImplementedError(
                f"PDF MAC only supports unpadded 256-bit AES key "
                f"wrapping for key encryption; not {kea_obj.dotted!r}."
            )

        encrypted_key = pwri['encrypted_key'].native
        try:
            mac_key = keywrap.aes_key_unwrap(
                wrapping_key=self.mac_kek, wrapped_key=encrypted_key
            )
        except keywrap.InvalidUnwrap:
            raise PdfMacValidationError("Failed to unwrap MAC key")

        return mac_key

    def _validate_message_digest(
        self, attrs: cms.CMSAttributes, encap_content: core.ParsableOctetString
    ):
        md_spec = get_pyca_cryptography_hash(self.md_algorithm)
        md = hashes.Hash(md_spec)
        md.update(bytes(encap_content))
        message_digest = md.finalize()
        try:
            claimed_message_digest = find_unique_cms_attribute(
                attrs, 'message_digest'
            )
            if claimed_message_digest.native != message_digest:
                raise PdfMacValidationError(
                    "Value of messageDigest attribute does not match hash of "
                    "encapsulated content."
                )
        except (NonexistentAttributeError, MultivaluedAttributeError):
            raise PdfMacValidationError(
                'Message digest not found in authenticated attributes, or '
                'multiple messageDigest attributes present.',
            )

    def _validate_auth_attrs(self, auth_data: cms.AuthenticatedData):
        auth_attrs = auth_data['auth_attrs']

        # (a) contentType
        _validate_content_type_attr(auth_attrs)

        # (b) CMSAlgorithmProtection
        try:
            validate_algorithm_protection(
                auth_attrs,
                claimed_signature_algorithm_obj=None,
                claimed_digest_algorithm_obj=auth_data['digest_algorithm'],
                claimed_mac_algorithm_obj=auth_data['mac_algorithm'],
            )
        except CMSAlgorithmProtectionError as e:
            raise PdfMacValidationError(
                "CMS alg protection validation error: " + e.failure_message,
            ) from e

        # (c) messageDigest
        int_info_obj = auth_data['encap_content_info']['content']
        self._validate_message_digest(auth_attrs, encap_content=int_info_obj)

    def _validate_and_extract_encap_content(
        self, auth_data: cms.AuthenticatedData
    ) -> PdfMacIntegrityInfo:
        # Verify lack of unauthenticated attributes
        unauth_attrs = auth_data['unauth_attrs']
        if unauth_attrs is not VOID:
            raise PdfMacValidationError(
                "PDF MAC tokens cannot have unauthenticated attributes"
            )

        # Compute the MAC of the authenticated attributes (after extracting
        # the MAC key from the unique PWRI in the payload)
        auth_attrs = auth_data['auth_attrs']
        recp_infos: cms.RecipientInfos = auth_data['recipient_infos']
        mac_key = self._retrieve_mac_key(recp_infos)

        computed_mac = self.compute_mac(
            mac_key, data_to_mac=auth_attrs.untag().dump()
        )
        if not compare_digest(computed_mac, auth_data['mac'].native):
            raise PdfMacValidationError("PDF MAC token has invalid MAC")

        # Now that we checked the MAC itself, we validate the content of
        # the authenticated attributes
        try:
            self._validate_auth_attrs(auth_data)
        except CMSStructuralError as e:
            raise PdfMacValidationError(
                "CMS structural error: " + e.failure_message
            ) from e

        # Return the encapsulated content after checking its content type
        eci_ct = auth_data['encap_content_info']['content_type'].native
        if eci_ct != 'pdf_mac_integrity_info':
            raise PdfMacValidationError(
                "The content type of the encapsulated content in a PDF MAC "
                "token must be id-pdfMacIntegrityInfo."
            )
        return auth_data['encap_content_info']['content'].parsed

    def validate_pdfmac_token_cms(
        self,
        *,
        auth_data: cms.AuthenticatedData,
        document_digest: bytes,
        signature_digest: Optional[bytes],
    ):
        encap_content = self._validate_and_extract_encap_content(auth_data)

        _validate_pdf_mac_integrity_info(
            encap_content, document_digest, signature_digest
        )


def _validate_pdf_mac_integrity_info(
    int_info: PdfMacIntegrityInfo,
    document_digest: bytes,
    signature_digest: Optional[bytes],
):
    claimed_data_digest = int_info['data_digest'].native
    if claimed_data_digest != document_digest:
        raise PdfMacValidationError(
            "Document digest does not match value in PdfMacIntegrityInfo"
        )

    sig_digest_obj = int_info['signature_digest']
    if signature_digest is None:
        if sig_digest_obj is not VOID:
            raise PdfMacValidationError(
                "PdfMacIntegrityInfo contains an unexpected signature digest"
            )
    else:
        claimed_signature_digest = sig_digest_obj.native
        if claimed_signature_digest is None:
            raise PdfMacValidationError(
                "Could not find signature digest in PdfMacIntegrityInfo"
            )
        if claimed_signature_digest != signature_digest:
            raise PdfMacValidationError(
                "Signature digest does not match value in PdfMacIntegrityInfo"
            )


def _validate_content_type_attr(auth_attrs):
    try:
        content_type = find_unique_cms_attribute(auth_attrs, 'content_type')

        if content_type.native != 'pdf_mac_integrity_info':
            raise PdfMacValidationError(
                'The content type attribute of a PDF MAC token must be '
                f'id-pdfMacIntegrityInfo, not {content_type.dotted!r}'
            )
    except (NonexistentAttributeError, MultivaluedAttributeError):
        raise PdfMacValidationError(
            'Content type not found in authenticated attributes, or '
            'multiple content-type attributes present.',
        )


def _validate_byte_range(pdf_dict, file_len, payload_len):
    try:
        byte_range = pdf_dict.raw_get('/ByteRange')
    except KeyError:
        byte_range = None
    if not isinstance(byte_range, generic.ArrayObject):
        raise PdfMacValidationError("No sensible /ByteRange found in AuthCode")

    if len(byte_range) == 4:
        o1, l1, o2, l2 = byte_range
        # same formula as for signatures
        value_lit_len = 2 * payload_len + 2
        if (
            o1 == 0
            and l1 + value_lit_len == o2
            and file_len == l1 + l2 + value_lit_len
        ):
            return

    raise PdfMacValidationError(
        "PDF MAC token must have /ByteRange covering the entire file"
    )


def _extract_standalone_mac(
    ac_dict: generic.DictionaryObject, file_len
) -> cms.ContentInfo:
    try:
        mac_bytes = ac_dict.raw_get('/MAC').original_bytes
    except (KeyError, AttributeError):
        raise PdfMacValidationError("Failed to retrieve standalone MAC value")

    mac_ci = cms.ContentInfo.load(mac_bytes)
    payload_len = len(mac_bytes)
    token_len = len(mac_ci.dump())
    if payload_len != token_len:
        raise PdfMacValidationError(
            f"Standalone MACs must not have trailing CMS data: "
            f"payload is {payload_len} bytes long, but token is {token_len} "
            f"bytes."
        )

    # validate the byte range while we're at it
    _validate_byte_range(ac_dict, file_len, payload_len)

    return mac_ci


def _extract_mac_in_sig(sig_dict, file_len) -> Tuple[cms.ContentInfo, bytes]:
    try:
        sig_bytes = extract_contents(sig_dict)
    except (KeyError, AttributeError, misc.PdfReadError):
        raise PdfMacValidationError("Failed to retrieve signature contents")

    sig_ci = cms.ContentInfo.load(sig_bytes)
    payload_len = len(sig_bytes)
    _validate_byte_range(sig_dict, file_len, payload_len)

    signer_info = None
    if sig_ci['content_type'].native == 'signed_data':
        sd = sig_ci['content']
        if len(sd['signer_infos']) == 1:
            signer_info = sd['signer_infos'][0]

    if signer_info is None:
        raise PdfMacValidationError(
            "Signature dictionary must contain a SignedData message "
            "with exactly 1 signerInfo."
        )

    try:
        mac_ci = find_unique_cms_attribute(
            signer_info['unsigned_attrs'], 'pdf_mac_data'
        )
    except (NonexistentAttributeError, MultivaluedAttributeError):
        raise PdfMacValidationError(
            'Signature must have exactly 1 pdfMacData unsigned attribute'
        )

    return mac_ci, signer_info['signature'].native


def validate_pdf_mac(
    reader: PdfFileReader,
    allowed_mds: FrozenSet[str] = ALLOWED_MD_ALGS,
    handler_cls: Type[PdfMacTokenHandler] = PdfMacTokenHandler,
):
    try:
        ac_dict = reader.trailer_view.raw_get('/AuthCode')
    except KeyError:
        ac_dict = None

    if isinstance(ac_dict, generic.IndirectObject):
        raise PdfMacValidationError("AuthCode dictionary cannot be indirect")
    if not isinstance(ac_dict, generic.DictionaryObject):
        raise PdfMacValidationError("Failed to locate AuthCode dictionary")

    is_standalone = False
    is_in_signature = False
    try:
        mac_location = ac_dict.raw_get('/MACLocation')
        if isinstance(mac_location, generic.NameObject):
            if mac_location == '/Standalone':
                is_standalone = True
            elif mac_location == '/AttachedToSig':
                is_in_signature = True
    except KeyError:
        pass

    file_len = reader.stream.seek(0, os.SEEK_END)
    if is_standalone:
        mac_ci = _extract_standalone_mac(ac_dict, file_len)
        # we've checked that /ByteRange is direct already, so this is OK
        byte_range = ac_dict['/ByteRange']
        signature_value = None
    elif is_in_signature:
        try:
            sig_ref = ac_dict.get_value_as_reference('/SigObjRef')
        except generic.IndirectObjectExpected:
            raise PdfMacValidationError(
                "Value of /SigObjRef entry must be an indirect reference"
            )
        except KeyError:
            raise PdfMacValidationError("/AttachedToSig requires /SigObjRef")
        sig_dict = sig_ref.get_object()
        if not isinstance(sig_dict, generic.DictionaryObject):
            raise PdfMacValidationError(
                "/SigObjRef does not point to a dictionary"
            )
        mac_ci, signature_value = _extract_mac_in_sig(sig_dict, file_len)
        byte_range = sig_dict['/ByteRange']
    else:
        raise PdfMacValidationError("Failed to locate MAC in document")

    if mac_ci['content_type'].native != 'authenticated_data':
        raise PdfMacValidationError(
            "MAC tokens must be of CMS type AuthenticatedData"
        )

    sh = reader.security_handler
    assert sh is not None
    auth_data = mac_ci['content']
    md_algorithm = auth_data['digest_algorithm']['algorithm'].native.lower()
    _, document_digest = byte_range_digest(
        reader.stream, byte_range=byte_range, md_algorithm=md_algorithm
    )
    if signature_value is not None:
        sig_md = hashes.Hash(get_pyca_cryptography_hash(md_algorithm))
        sig_md.update(signature_value)
        signature_digest = sig_md.finalize()
    else:
        signature_digest = None

    try:
        salt = sh.get_kdf_salt()
    except misc.PdfReadError as e:
        raise PdfMacValidationError("Error retrieving salt") from e

    handler_cls.for_validation(
        file_encryption_key=sh.get_file_encryption_key(),
        kdf_salt=salt,
        auth_data=auth_data,
        allowed_mds=allowed_mds,
    ).validate_pdfmac_token_cms(
        auth_data=auth_data,
        document_digest=document_digest,
        signature_digest=signature_digest,
    )


class StandalonePdfMac(PdfByteRangeDigest):
    def __init__(self, *, bytes_reserved):
        super().__init__(
            data_key=pdf_name('/MAC'), bytes_reserved=bytes_reserved
        )
        self['/MACLocation'] = pdf_name('/Standalone')


def add_standalone_mac(
    w: BasePdfFileWriter,
    md_algorithm: str = 'sha256',
    in_place=False,
    output=None,
    chunk_size=misc.DEFAULT_CHUNK_SIZE,
):
    handler: PdfMacTokenHandler = w._init_mac_handler(md_algorithm=md_algorithm)
    tok_size = handler.determine_token_size(
        include_signature_digest=False,
    )
    mac_dict = StandalonePdfMac(bytes_reserved=2 * tok_size)
    w.set_custom_trailer_entry(pdf_name('/AuthCode'), mac_dict)

    prepared_br_digest: PreparedByteRangeDigest
    cms_writer = mac_dict.fill(
        w,
        # here we intentionally take the handlers MD (more convenient
        # to override in tests)
        md_algorithm=handler.md_algorithm,
        in_place=in_place,
        chunk_size=chunk_size,
        output=output,
    )

    prepared_br_digest, res_output = next(cms_writer)
    pdfmac_token = handler.build_pdfmac_token(
        document_digest=prepared_br_digest.document_digest,
        signature_digest=None,
    )
    prepared_br_digest.fill_with_cms(res_output, cms_data=pdfmac_token)

    return misc.finalise_output(output, res_output)
