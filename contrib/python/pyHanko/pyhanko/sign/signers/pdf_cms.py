"""
This module defines utility classes to format CMS objects for use in PDF
signatures.
"""

import asyncio
import logging
import warnings
from dataclasses import dataclass
from datetime import datetime
from typing import IO, Callable, Iterable, List, Optional, Union

import tzlocal
from asn1crypto import algos, cms, core, keys
from asn1crypto import pdf as asn1_pdf
from asn1crypto import x509
from asn1crypto.algos import SignedDigestAlgorithm
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.dsa import DSAPrivateKey
from cryptography.hazmat.primitives.asymmetric.ec import (
    ECDSA,
    EllipticCurvePrivateKey,
)
from cryptography.hazmat.primitives.asymmetric.ed448 import Ed448PrivateKey
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.hazmat.primitives.serialization import pkcs12
from pyhanko_certvalidator._asyncio_compat import to_thread
from pyhanko_certvalidator.registry import (
    CertificateStore,
    SimpleCertificateStore,
)

from pyhanko.pdf_utils import misc
from pyhanko.sign import attributes
from pyhanko.sign.ades.api import CAdESSignedAttrSpec
from pyhanko.sign.attributes import (
    CMSAttributeProvider,
    SignedAttributeProviderSpec,
    UnsignedAttributeProviderSpec,
)
from pyhanko.sign.general import (
    SigningError,
    get_cms_hash_algo_for_mechanism,
    get_pyca_cryptography_hash,
    optimal_pss_params,
    process_pss_params,
    simple_cms_attribute,
)
from pyhanko.sign.timestamps import TimeStamper

from ...keys import (
    _translate_pyca_cryptography_cert_to_asn1,
    _translate_pyca_cryptography_key_to_asn1,
    load_cert_from_pemder,
    load_certs_from_pemder,
    load_private_key_from_pemder,
)
from . import constants

__all__ = [
    'Signer',
    'SimpleSigner',
    'ExternalSigner',
    'PdfCMSSignedAttributes',
    'format_attributes',
    'format_signed_attributes',
    'asyncify_signer',
    'select_suitable_signing_md',
    'signer_from_p12_config',
    'signer_from_pemder_config',
]

from ...config.errors import ConfigurationError
from ...config.local_keys import PemDerSignatureConfig, PKCS12SignatureConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CMSSignedAttributes:
    """
    .. versionadded:: 0.14.0

    Serialisable container class describing input for various signed attributes
    in a signature CMS object.
    """

    signing_time: Optional[datetime] = None
    """
    Timestamp for the ``signingTime`` attribute. Will be ignored in a PAdES
    context.
    """

    cades_signed_attrs: Optional[CAdESSignedAttrSpec] = None
    """
    Optional settings for CAdES-style signed attributes.
    """


@dataclass(frozen=True)
class PdfCMSSignedAttributes(CMSSignedAttributes):
    """
    .. versionadded:: 0.7.0

    .. versionchanged:: 0.14.0
        Split off some fields into :class:`.CMSSignedAttributes`.


    Serialisable container class describing input for various signed attributes
    in a CMS object for a PDF signature.
    """

    adobe_revinfo_attr: Optional[asn1_pdf.RevocationInfoArchival] = None
    """
    Adobe-style signed revocation info attribute.
    """


def _ensure_content_type(encap_content_info):
    encap_content_info = encap_content_info or {'content_type': 'data'}
    if isinstance(encap_content_info, core.Sequence):
        # could be cms.ContentInfo or cms.EncapsulatedContentInfo depending
        # on circumstances, so let's just stick to Sequence
        content_type = encap_content_info['content_type'].native
    else:
        content_type = encap_content_info.get('content_type', 'data')

    return encap_content_info, content_type


def _cms_version(
    content_type: Union[str, core.ObjectIdentifier], has_attribute_certs
):
    if has_attribute_certs:
        return 'v4'

    if isinstance(content_type, core.ObjectIdentifier):
        content_type = content_type.native

    # We don't support signing with subjectKeyIdentifier, so there's no
    # need to take that into account here. We also don't do version 1 attribute
    # certificates.
    # (these matter for distinguishing v1<>v3)
    return 'v1' if content_type == 'data' else 'v3'


def _prepare_encap_content(
    input_data: Union[IO, bytes, cms.ContentInfo, cms.EncapsulatedContentInfo],
    digest_algorithm: str,
    detached=True,
    chunk_size=misc.DEFAULT_CHUNK_SIZE,
    max_read=None,
):
    h = hashes.Hash(get_pyca_cryptography_hash(digest_algorithm))
    encap_content_info = None
    if isinstance(input_data, (cms.ContentInfo, cms.EncapsulatedContentInfo)):
        h.update(bytes(input_data['content']))
        if detached:
            encap_content_info = {'content_type': input_data['content_type']}
        else:
            encap_content_info = input_data
    elif isinstance(input_data, bytes):
        h.update(input_data)
        if not detached:
            # use dicts instead of Asn1Value objects, to leave asn1crypto
            # to decide whether to use cms.ContentInfo or
            # cms.EncapsulatedContentInfo (for backwards compat with PCKS#7)
            encap_content_info = {'content_type': 'data', 'content': input_data}
    elif not detached:
        # input stream is a buffer, and we're in 'enveloping' mode
        # read the entire thing into memory, since we need to embed
        # it anyway
        input_bytes = input_data.read(max_read)
        h.update(input_bytes)
        # see above
        encap_content_info = {'content_type': 'data', 'content': input_bytes}
    else:
        temp_buf = bytearray(chunk_size)
        misc.chunked_digest(temp_buf, input_data, h, max_read=max_read)
    digest_bytes = h.finalize()
    return encap_content_info, digest_bytes


async def format_attributes(
    attr_provs: List[CMSAttributeProvider],
    other_attrs: Iterable[cms.CMSAttributes] = (),
    dry_run: bool = False,
) -> cms.CMSAttributes:
    """
    Format CMS attributes obtained from attribute providers.

    :param attr_provs:
        List of attribute providers.
    :param other_attrs:
        Other (predetermined) attributes to include.
    :param dry_run:
        Whether to invoke the attribute providers in dry-run mode or not.
    :return:
        A :class:`cms.CMSAttributes` value.
    """

    attrs = list(other_attrs)
    jobs = [prov.get_attribute(dry_run=dry_run) for prov in attr_provs]
    for attr_coro in asyncio.as_completed(jobs):
        attr = await attr_coro
        if attr is not None:
            attrs.append(attr)

    return cms.CMSAttributes(attrs)


async def format_signed_attributes(
    data_digest: bytes,
    attr_provs: List[CMSAttributeProvider],
    content_type='data',
    dry_run=False,
) -> cms.CMSAttributes:
    """
    Format signed attributes for a CMS ``SignerInfo`` value.

    :param data_digest:
        The byte string to put in the ``messageDigest`` attribute.
    :param attr_provs:
        List of attribute providers to source attributes from.
    :param content_type:
        The content type of the data being signed (default is ``data``).
    :param dry_run:
        Whether to invoke the attribute providers in dry-run mode or not.
    :return:
        A :class:`cms.CMSAttributes` value representing the signed attributes.
    """
    attrs = [
        simple_cms_attribute('content_type', content_type),
        simple_cms_attribute('message_digest', data_digest),
    ]
    return await format_attributes(
        attr_provs, dry_run=dry_run, other_attrs=attrs
    )


class Signer:
    """
    Abstract signer object that is agnostic as to where the cryptographic
    operations actually happen.

    As of now, pyHanko provides two implementations:

    * :class:`.SimpleSigner` implements the easy case where all the key material
      can be loaded into memory.
    * :class:`~pyhanko.sign.pkcs11.PKCS11Signer` implements a signer that is
      capable of interfacing with a PKCS#11 device.

    :param prefer_pss:
        When signing using an RSA key, prefer PSS padding to legacy PKCS#1 v1.5
        padding. Default is ``False``. This option has no effect on non-RSA
        signatures.
    :param embed_roots:
        .. versionadded:: 0.9.0

        Option that controls whether or not additional self-signed certificates
        should be embedded into the CMS payload. The default is ``True``.

        .. note::
            The signer's certificate is always embedded, even if it is
            self-signed.

        .. note::
            Trust roots are configured by the validator, so embedding them
            doesn't affect the semantics of a typical validation process.
            Therefore, they can be safely omitted in most cases.
            Nonetheless, embedding the roots can be useful for documentation
            purposes. In addition, some validators are poorly implemented,
            and will refuse to build paths if the roots are not present
            in the file.

        .. warning::
            To be precise, if this flag is ``False``, a certificate will be
            dropped if (a) it is not the signer's, (b) it is self-issued and
            (c) its subject and authority key identifiers match (or either is
            missing). In other words, we never validate the actual
            self-signature. This heuristic is sufficiently accurate
            for most applications.
    :param signature_mechanism:
        The (cryptographic) signature mechanism to use for all signing
        operations. If unset, the default behaviour is to try to impute
        a reasonable one given the preferred digest algorithm and public key.
    :param signing_cert:
        See :attr:`signing_cert`.
    :param attribute_certs:
        See :attr:`attribute_certs`.
    :param cert_registry:
        Initial value for :attr:`cert_registry`. If unset, an empty certificate
        store will be initialised.
    """

    def __init__(
        self,
        *,
        prefer_pss: bool = False,
        embed_roots: bool = True,
        signature_mechanism: Optional[SignedDigestAlgorithm] = None,
        signing_cert: Optional[x509.Certificate] = None,
        cert_registry: Optional[CertificateStore] = None,
        attribute_certs: Iterable[cms.AttributeCertificateV2] = (),
    ):
        self.prefer_pss = prefer_pss
        self.embed_roots = embed_roots
        self.signed_attr_prov_spec: Optional[SignedAttributeProviderSpec] = None
        self.unsigned_attr_prov_spec: Optional[
            UnsignedAttributeProviderSpec
        ] = None
        self._signature_mechanism = signature_mechanism
        self._signing_cert = signing_cert
        self._cert_registry = cert_registry or SimpleCertificateStore()
        self._attribute_certs = attribute_certs

    @property
    def signature_mechanism(self) -> Optional[SignedDigestAlgorithm]:
        """
        .. versionchanged:: 0.18.0
            Turned into a property instead of a class attribute.

        The (cryptographic) signature mechanism to use for all signing
        operations.
        """
        return self._signature_mechanism

    @property
    def signing_cert(self) -> Optional[x509.Certificate]:
        """
        .. versionchanged:: 0.14.0
            Made optional (see note)

        .. versionchanged:: 0.18.0
            Turned into a property instead of a class attribute.

        The certificate that will be used to create the signature.

        .. note::
            This is an optional field only to a limited extent. Subclasses may
            require it to be present, and not setting it at the beginning of
            the signing process implies that certain high-level convenience
            features will no longer work or be limited in function (e.g.,
            automatic hash selection, appearance generation, revocation
            information collection, ...).

            However, making :attr:`signing_cert` optional enables certain
            signing workflows where the certificate of the signer is not known
            until the signature has actually been produced. This is most
            relevant in certain types of remote signing scenarios.
        """
        return self._signing_cert

    @property
    def cert_registry(self) -> CertificateStore:
        """
        .. versionchanged:: 0.18.0
            Turned into a property instead of a class attribute.

        Collection of certificates associated with this signer.
        Note that this is simply a bookkeeping tool; in particular it
        doesn't care about trust.
        """
        return self._cert_registry

    @property
    def attribute_certs(self) -> Iterable[cms.AttributeCertificateV2]:
        """
        .. versionchanged:: 0.18.0
            Turned into a property instead of a class attribute.

        Attribute certificates to include with the signature.

        .. note::
            Only ``v2`` attribute certificates are supported.
        """
        return self._attribute_certs

    def get_signature_mechanism_for_digest(
        self, digest_algorithm: Optional[str]
    ) -> SignedDigestAlgorithm:
        """
        Get the signature mechanism for this signer to use.
        If :attr:`signature_mechanism` is set, it will be used.
        Otherwise, this method will attempt to put together a default
        based on mechanism used in the signer's certificate.

        :param digest_algorithm:
            Digest algorithm to use as part of the signature mechanism.
            Only used if a signature mechanism object has to be put together
            on-the-fly.
        :return:
            A :class:`.SignedDigestAlgorithm` object.
        """

        if self.signature_mechanism is not None:
            return self.signature_mechanism
        if self.signing_cert is None:
            raise SigningError(
                "Could not set up a default signature mechanism."
            )
        # Grab the certificate's algorithm (but forget about the digest)
        #  and use that to set up the default.
        # We'll specify the digest somewhere else.
        algo = self.signing_cert.public_key.algorithm
        params = None
        if algo == 'ec':
            # with ECDSA, RFC 5753 requires us to encode the digest
            # algorithm together with the signing algorithm.
            # The correspondence with the digestAlgorithm field in CMS is
            # verified separately.
            if digest_algorithm is None:
                raise ValueError("Digest algorithm required for ECDSA")
            mech = digest_algorithm + '_ecdsa'
        elif algo == 'dsa':
            if digest_algorithm is None:
                raise ValueError("Digest algorithm required for DSA")
            # Note: DSA isn't specified with sha384 and sha512, but we
            # don't check that here; let's allow the error to percolate
            # further down for now.
            # TODO (but maybe it's worth revisiting the issue of checking
            #  signature <> hash algorithm combinations in greater generality)
            mech = digest_algorithm + '_dsa'
        elif algo == 'rsa':
            if self.prefer_pss:
                mech = 'rsassa_pss'
                if digest_algorithm is None:
                    raise ValueError("Digest algorithm required for RSASSA-PSS")
                params = optimal_pss_params(self.signing_cert, digest_algorithm)
            elif digest_algorithm is not None:
                mech = digest_algorithm + '_rsa'
            else:
                mech = 'rsassa_pkcs1v15'
        elif algo in ('ed25519', 'ed448'):
            mech = algo
        else:  # pragma: nocover
            raise SigningError(f"Signature mechanism {algo} is unsupported.")

        sda_kwargs = {'algorithm': mech}
        if params is not None:
            sda_kwargs['parameters'] = params
        return SignedDigestAlgorithm(sda_kwargs)

    @property
    def subject_name(self) -> Optional[str]:
        """
        :return:
            The subject's common name as a string, extracted from
            :attr:`signing_cert`, or ``None`` if no signer's certificate is
            available
        """
        if self.signing_cert is None:
            return None

        name: x509.Name = self.signing_cert.subject
        try:
            result = name.native['common_name']
        except KeyError:
            result = name.native['organization_name']
        try:
            email = name.native['email_address']
            result = '%s <%s>' % (result, email)
        except KeyError:
            pass
        return result

    @staticmethod
    def format_revinfo(
        ocsp_responses: Optional[list] = None, crls: Optional[list] = None
    ):
        """
        Format Adobe-style revocation information for inclusion into a CMS
        object.

        :param ocsp_responses:
            A list of OCSP responses to include.
        :param crls:
            A list of CRLs to include.
        """

        revinfo_dict = {}
        if ocsp_responses:
            revinfo_dict['ocsp'] = ocsp_responses

        if crls:
            revinfo_dict['crl'] = crls

        if revinfo_dict:
            return asn1_pdf.RevocationInfoArchival(revinfo_dict)

    def _signed_attr_provider_spec(
        self,
        attr_settings: PdfCMSSignedAttributes,
        timestamper=None,
        use_cades=False,
        is_pdf_sig=True,
    ):
        """
        Internal method to select a default attribute provider spec if none
        is available already.
        """

        if self.signed_attr_prov_spec is not None:
            return self.signed_attr_prov_spec
        elif use_cades:
            return CAdESSignedAttributeProviderSpec(
                attr_settings=attr_settings,
                signing_cert=self.signing_cert,
                is_pades=is_pdf_sig,
                timestamper=timestamper,
            )
        elif is_pdf_sig:
            return GenericPdfSignedAttributeProviderSpec(
                attr_settings=attr_settings,
                signing_cert=self.signing_cert,
                signature_mechanism=self.get_signature_mechanism_for_digest,
                timestamper=timestamper,
            )
        else:
            return GenericCMSSignedAttributeProviderSpec(
                attr_settings=attr_settings,
                signing_cert=self.signing_cert,
                signature_mechanism=self.get_signature_mechanism_for_digest,
                timestamper=timestamper,
            )

    def _signed_attr_providers(
        self,
        data_digest: bytes,
        digest_algorithm: str,
        attr_settings: PdfCMSSignedAttributes,
        timestamper=None,
        use_cades=False,
        is_pdf_sig=True,
    ):
        """
        Prepare "standard" signed attribute providers. Internal API.
        """

        spec = self._signed_attr_provider_spec(
            attr_settings=attr_settings,
            timestamper=timestamper,
            use_cades=use_cades,
            is_pdf_sig=is_pdf_sig,
        )

        return spec.signed_attr_providers(
            data_digest=data_digest,
            digest_algorithm=digest_algorithm,
        )

    def _unsigned_attr_provider_spec(
        self, timestamper: Optional[TimeStamper] = None
    ):
        if self.unsigned_attr_prov_spec is not None:
            return self.unsigned_attr_prov_spec
        else:
            return DefaultUnsignedAttributes(timestamper=timestamper)

    def _unsigned_attr_providers(
        self,
        digest_algorithm: str,
        signature: bytes,
        signed_attrs: cms.CMSAttributes,
        timestamper: Optional[TimeStamper] = None,
    ):
        """
        Prepare "standard" unsigned attribute providers. Internal API.
        """
        spec = self._unsigned_attr_provider_spec(timestamper)
        return spec.unsigned_attr_providers(
            digest_algorithm=digest_algorithm,
            signature=signature,
            signed_attrs=signed_attrs,
        )

    def signer_info(self, digest_algorithm: str, signed_attrs, signature):
        """
        Format the ``SignerInfo`` entry for a CMS signature.

        :param digest_algorithm:
            Digest algorithm to use.
        :param signed_attrs:
            Signed attributes (see :meth:`signed_attrs`).
        :param signature:
            The raw signature to embed (see :meth:`sign_raw`).
        :return:
            An :class:`.asn1crypto.cms.SignerInfo` object.
        """

        digest_algorithm_args = {'algorithm': digest_algorithm}
        if digest_algorithm == 'shake256':
            # RFC 8419 requirement
            mech = self.get_signature_mechanism_for_digest('shake256')
            if mech['algorithm'].native == 'ed448':
                digest_algorithm_args = {
                    'algorithm': 'shake256_len',
                    'parameters': core.Integer(512),
                }
        digest_algorithm_obj = algos.DigestAlgorithm(digest_algorithm_args)

        signing_cert = self.signing_cert
        if signing_cert is None:
            raise SigningError(
                "The signer\'s certificate must be available for "
                "SignerInfo assembly to proceed."
            )
        # build the signer info object that goes into the PKCS7 signature
        # (see RFC 2315 § 9.2)
        sig_info = cms.SignerInfo(
            {
                'version': 'v1',
                'sid': cms.SignerIdentifier(
                    {
                        'issuer_and_serial_number': cms.IssuerAndSerialNumber(
                            {
                                'issuer': signing_cert.issuer,
                                'serial_number': signing_cert.serial_number,
                            }
                        )
                    }
                ),
                'digest_algorithm': digest_algorithm_obj,
                'signature_algorithm': self.get_signature_mechanism_for_digest(
                    digest_algorithm
                ),
                'signed_attrs': signed_attrs,
                'signature': signature,
            }
        )
        return sig_info

    def _package_signature(
        self,
        *,
        digest_algorithm: str,
        cms_version,
        signed_attrs: cms.CMSAttributes,
        signature: bytes,
        unsigned_attrs: cms.CMSAttributes,
        encap_content_info,
    ) -> cms.ContentInfo:
        encap_content_info = encap_content_info or {'content_type': 'data'}
        sig_info = self.signer_info(digest_algorithm, signed_attrs, signature)
        digest_algorithm_obj = sig_info['digest_algorithm']

        if unsigned_attrs is not None:
            sig_info['unsigned_attrs'] = unsigned_attrs

        cert_registry = self.cert_registry or ()
        # Note: we do not add the TS certs at this point
        if self.embed_roots:
            certs = {
                cms.CertificateChoices(name='certificate', value=cert)
                for cert in cert_registry
            }
        else:
            # asn1crypto's heuristic is good enough for now, we won't check the
            # actual signatures. CAs that make use of self-issued certificates
            # for things like key rollover probably also use SKI/AKI to
            # distinguish between different certs, which will be picked up by
            # asn1crypto either way.
            certs = {
                cms.CertificateChoices(name='certificate', value=cert)
                for cert in cert_registry
                if cert.self_signed == 'no'
            }
        certs.add(
            cms.CertificateChoices(name='certificate', value=self.signing_cert)
        )

        # add attribute certs
        for ac in self.attribute_certs:
            certs.add(cms.CertificateChoices(name='v2_attr_cert', value=ac))

        # this is the SignedData object for our message (see RFC 2315 § 9.1)
        signed_data = {
            'version': cms_version,
            'digest_algorithms': cms.DigestAlgorithms((digest_algorithm_obj,)),
            'encap_content_info': encap_content_info,
            'certificates': certs,
            'signer_infos': [sig_info],
        }

        return cms.ContentInfo(
            {
                'content_type': cms.ContentType('signed_data'),
                'content': cms.SignedData(signed_data),
            }
        )

    def _check_digest_algorithm(self, digest_algorithm: str):
        implied_hash_algo = None
        try:
            # Just using self.signature_mechanism is not good enough,
            # we need to cover cases like ed448 and ed25519 (where
            # the hash algorithm choice is fixed) also when the signature
            # mechanism is not explicitly passed in.
            mech = self.get_signature_mechanism_for_digest(None)
        except ValueError:
            # This could happen if there's no explicit mechanism defined,
            # and we're signing with ECDSA, for example. In that case
            # we simply use the digest algorithm passed in.
            mech = None
        try:
            if mech is not None:
                implied_hash_algo = get_cms_hash_algo_for_mechanism(mech)
        except ValueError:
            # this is OK, just use the specified message digest
            pass
        if (
            implied_hash_algo is not None
            and implied_hash_algo != digest_algorithm
        ):
            raise SigningError(
                f"Selected signature mechanism specifies message digest "
                f"{implied_hash_algo}, but {digest_algorithm} "
                f"was requested."
            )

    async def async_sign_raw(
        self, data: bytes, digest_algorithm: str, dry_run=False
    ) -> bytes:
        """
        Compute the raw cryptographic signature of the data provided, hashed
        using the digest algorithm provided.

        :param data:
            Data to sign.
        :param digest_algorithm:
            Digest algorithm to use.

            .. warning::
                If :attr:`signature_mechanism` also specifies a digest, they
                should match.
        :param dry_run:
            Do not actually create a signature, but merely output placeholder
            bytes that would suffice to contain an actual signature.
        :return:
            Signature bytes.
        """
        raise NotImplementedError

    async def unsigned_attrs(
        self,
        digest_algorithm: str,
        signature: bytes,
        signed_attrs: cms.CMSAttributes,
        timestamper=None,
        dry_run=False,
    ) -> Optional[cms.CMSAttributes]:
        """
        .. versionchanged:: 0.9.0
            Made asynchronous _(breaking change)_

        .. versionchanged:: 0.14.0
            Added ``signed_attrs`` parameter _(breaking change)_

        Compute the unsigned attributes to embed into the CMS object.
        This function is called after signing the hash of the signed attributes
        (see :meth:`signed_attrs`).

        By default, this method only handles timestamp requests, but other
        functionality may be added by subclasses

        If this method returns ``None``, no unsigned attributes will be
        embedded.

        :param digest_algorithm:
            Digest algorithm used to hash the signed attributes.
        :param signed_attrs:
            Signed attributes of the signature.
        :param signature:
            Signature of the signed attribute hash.
        :param timestamper:
            Timestamp supplier to use.
        :param dry_run:
            Flag indicating "dry run" mode. If ``True``, only the approximate
            size of the output matters, so cryptographic
            operations can be replaced by placeholders.
        :return:
            The unsigned attributes to add, or ``None``.
        """
        provs = self._unsigned_attr_providers(
            signature=signature,
            signed_attrs=signed_attrs,
            digest_algorithm=digest_algorithm,
            timestamper=timestamper,
        )
        attrs = await format_attributes(list(provs), dry_run=dry_run)
        return attrs or None

    async def signed_attrs(
        self,
        data_digest: bytes,
        digest_algorithm: str,
        attr_settings: Optional[PdfCMSSignedAttributes] = None,
        content_type='data',
        use_pades=False,
        timestamper=None,
        dry_run=False,
        is_pdf_sig=True,
    ):
        """
        .. versionchanged:: 0.4.0
            Added positional ``digest_algorithm`` parameter _(breaking change)_.
        .. versionchanged:: 0.5.0
            Added ``dry_run``, ``timestamper`` and ``cades_meta`` parameters.
        .. versionchanged:: 0.9.0
            Made asynchronous, grouped some parameters under ``attr_settings``
            _(breaking change)_

        Format the signed attributes for a CMS signature.

        :param data_digest:
            Raw digest of the data to be signed.
        :param digest_algorithm:
            .. versionadded:: 0.4.0

            Name of the digest algorithm used to compute the digest.
        :param use_pades:
            Respect PAdES requirements.
        :param dry_run:
            .. versionadded:: 0.5.0

            Flag indicating "dry run" mode. If ``True``, only the approximate
            size of the output matters, so cryptographic
            operations can be replaced by placeholders.
        :param attr_settings:
            :class:`.PdfCMSSignedAttributes` object describing the attributes
            to be added.
        :param timestamper:
            .. versionadded:: 0.5.0

            Timestamper to use when creating timestamp tokens.
        :param content_type:
            CMS content type of the encapsulated data. Default is `data`.

            .. danger::
                This parameter is internal API, and non-default values must not
                be used to produce PDF signatures.
        :param is_pdf_sig:
            Whether the signature being generated is for use in a PDF document.

            .. danger::
                This parameter is internal API.
        :return:
            An :class:`.asn1crypto.cms.CMSAttributes` object.
        """

        attr_settings = attr_settings or PdfCMSSignedAttributes()

        provs = self._signed_attr_providers(
            data_digest=data_digest,
            digest_algorithm=digest_algorithm,
            use_cades=use_pades,
            attr_settings=attr_settings,
            timestamper=timestamper,
            is_pdf_sig=is_pdf_sig,
        )

        return await format_signed_attributes(
            data_digest,
            attr_provs=list(provs),
            content_type=content_type,
            dry_run=dry_run,
        )

    async def async_sign(
        self,
        data_digest: bytes,
        digest_algorithm: str,
        dry_run=False,
        use_pades=False,
        timestamper=None,
        signed_attr_settings: Optional[PdfCMSSignedAttributes] = None,
        is_pdf_sig=True,
        encap_content_info=None,
    ) -> cms.ContentInfo:
        """
        .. versionadded:: 0.9.0

        Produce a detached CMS signature from a raw data digest.

        :param data_digest:
            Digest of the actual content being signed.
        :param digest_algorithm:
            Digest algorithm to use. This should be the same digest method
            as the one used to hash the (external) content.
        :param dry_run:
            If ``True``, the actual signing step will be replaced with
            a placeholder.

            In a PDF signing context, this is necessary to estimate the size
            of the signature container before computing the actual digest of
            the document.
        :param signed_attr_settings:
            :class:`.PdfCMSSignedAttributes` object describing the attributes
            to be added.
        :param use_pades:
            Respect PAdES requirements.
        :param timestamper:
            :class:`~.timestamps.TimeStamper` used to obtain a trusted timestamp
            token that can be embedded into the signature container.

            .. note::
                If ``dry_run`` is true, the timestamper's
                :meth:`~.timestamps.TimeStamper.dummy_response` method will be
                called to obtain a placeholder token.
                Note that with a standard :class:`~.timestamps.HTTPTimeStamper`,
                this might still hit the timestamping server (in order to
                produce a realistic size estimate), but the dummy response will
                be cached.
        :param is_pdf_sig:
            Whether the signature being generated is for use in a PDF document.

            .. danger::
                This parameter is internal API.
        :param encap_content_info:
            Data to encapsulate in the CMS object.

            .. danger::
                This parameter is internal API, and must not be used to produce
                PDF signatures.
        :return:
            An :class:`~.asn1crypto.cms.ContentInfo` object.
        """

        encap_content_info, content_type = _ensure_content_type(
            encap_content_info
        )
        signed_attrs = await self.signed_attrs(
            data_digest,
            digest_algorithm,
            attr_settings=signed_attr_settings,
            use_pades=use_pades,
            timestamper=timestamper,
            dry_run=dry_run,
            content_type=content_type,
            is_pdf_sig=is_pdf_sig,
        )
        return await self.async_sign_prescribed_attributes(
            digest_algorithm,
            signed_attrs,
            cms_version=_cms_version(content_type, bool(self.attribute_certs)),
            dry_run=dry_run,
            timestamper=timestamper,
            encap_content_info=encap_content_info,
        )

    async def async_sign_prescribed_attributes(
        self,
        digest_algorithm: str,
        signed_attrs: cms.CMSAttributes,
        cms_version='v1',
        dry_run=False,
        timestamper=None,
        encap_content_info=None,
    ) -> cms.ContentInfo:
        """
        .. versionadded:: 0.9.0

        Start the CMS signing process with the prescribed set of signed
        attributes.

        :param digest_algorithm:
            Digest algorithm to use. This should be the same digest method
            as the one used to hash the (external) content.
        :param signed_attrs:
            CMS attributes to sign.
        :param dry_run:
            If ``True``, the actual signing step will be replaced with
            a placeholder.

            In a PDF signing context, this is necessary to estimate the size
            of the signature container before computing the actual digest of
            the document.
        :param timestamper:
            :class:`~.timestamps.TimeStamper` used to obtain a trusted timestamp
            token that can be embedded into the signature container.

            .. note::
                If ``dry_run`` is true, the timestamper's
                :meth:`~.timestamps.TimeStamper.dummy_response` method will be
                called to obtain a placeholder token.
                Note that with a standard :class:`~.timestamps.HTTPTimeStamper`,
                this might still hit the timestamping server (in order to
                produce a realistic size estimate), but the dummy response will
                be cached.
        :param cms_version:
            CMS version to use.
        :param encap_content_info:
            Data to encapsulate in the CMS object.

            .. danger::
                This parameter is internal API, and must not be used to produce
                PDF signatures.
        :return:
            An :class:`~.asn1crypto.cms.ContentInfo` object.
        """

        digest_algorithm = digest_algorithm.lower()
        self._check_digest_algorithm(digest_algorithm)

        signature = await self.async_sign_raw(
            signed_attrs.dump(), digest_algorithm.lower(), dry_run
        )
        unsigned_attrs = await self.unsigned_attrs(
            digest_algorithm,
            signature,
            signed_attrs=signed_attrs,
            timestamper=timestamper,
            dry_run=dry_run,
        )
        return self._package_signature(
            digest_algorithm=digest_algorithm,
            cms_version=cms_version,
            signed_attrs=signed_attrs,
            signature=signature,
            unsigned_attrs=unsigned_attrs,
            encap_content_info=encap_content_info,
        )

    async def async_sign_general_data(
        self,
        input_data: Union[
            IO, bytes, cms.ContentInfo, cms.EncapsulatedContentInfo
        ],
        digest_algorithm: str,
        detached=True,
        use_cades=False,
        timestamper=None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
        signed_attr_settings: Optional[PdfCMSSignedAttributes] = None,
        max_read=None,
    ) -> cms.ContentInfo:
        """
        .. versionadded:: 0.9.0

        Produce a CMS signature for an arbitrary data stream
        (not necessarily PDF data).

        :param input_data:
            The input data to sign. This can be either a :class:`bytes` object
            a file-type object, a :class:`cms.ContentInfo` object or
            a :class:`cms.EncapsulatedContentInfo` object.

            .. warning::
                ``asn1crypto`` mandates :class:`cms.ContentInfo` for CMS v1
                signatures. In practical terms, this means that you need to
                use :class:`cms.ContentInfo` if the content type is ``data``,
                and :class:`cms.EncapsulatedContentInfo` otherwise.

            .. warning::
                We currently only support CMS v1, v3 and v4 signatures.
                This is only a concern if you need certificates or CRLs
                of type 'other', in which case you can change the version
                yourself (this will not invalidate any signatures).
                You'll also need to do this if you need support for version 1
                attribute certificates, or if you want to sign with
                ``subjectKeyIdentifier`` in the ``sid`` field.
        :param digest_algorithm:
            The name of the digest algorithm to use.
        :param detached:
            If ``True``, create a CMS detached signature (i.e. an object where
            the encapsulated content is not embedded in the signature object
            itself). This is the default. If ``False``, the content to be
            signed will be embedded as encapsulated content.
        :param signed_attr_settings:
            :class:`.PdfCMSSignedAttributes` object describing the attributes
            to be added.
        :param use_cades:
            Construct a CAdES-style CMS object.
        :param timestamper:
            :class:`.PdfTimeStamper` to use to create a signature timestamp

            .. note::
                If you want to create a *content* timestamp (as opposed to
                a *signature* timestamp), see :class:`.CAdESSignedAttrSpec`.
        :param chunk_size:
            Chunk size to use when consuming input data.
        :param max_read:
            Maximal number of bytes to read from the input stream.
        :return:
            A CMS ContentInfo object of type signedData.
        """

        encap_content_info, digest_bytes = _prepare_encap_content(
            input_data=input_data,
            digest_algorithm=digest_algorithm,
            detached=detached,
            chunk_size=chunk_size,
            max_read=max_read,
        )
        return await self.async_sign(
            data_digest=digest_bytes,
            digest_algorithm=digest_algorithm,
            use_pades=use_cades,
            is_pdf_sig=False,
            timestamper=timestamper,
            encap_content_info=encap_content_info,
            signed_attr_settings=signed_attr_settings,
        )

    def sign(
        self,
        data_digest: bytes,
        digest_algorithm: str,
        timestamp: Optional[datetime] = None,
        dry_run=False,
        revocation_info=None,
        use_pades=False,
        timestamper=None,
        cades_signed_attr_meta: Optional[CAdESSignedAttrSpec] = None,
        encap_content_info=None,
    ) -> cms.ContentInfo:
        """
        .. deprecated:: 0.9.0
            Use :meth:`async_sign` instead.
            The implementation of this method will invoke :meth:`async_sign`
            using ``asyncio.run()``.

        Produce a detached CMS signature from a raw data digest.

        :param data_digest:
            Digest of the actual content being signed.
        :param digest_algorithm:
            Digest algorithm to use. This should be the same digest method
            as the one used to hash the (external) content.
        :param timestamp:
            Signing time to embed into the signed attributes
            (will be ignored if ``use_pades`` is ``True``).

            .. note::
                This timestamp value is to be interpreted as an unfounded
                assertion by the signer, which may or may not be good enough
                for your purposes.
        :param dry_run:
            If ``True``, the actual signing step will be replaced with
            a placeholder.

            In a PDF signing context, this is necessary to estimate the size
            of the signature container before computing the actual digest of
            the document.
        :param revocation_info:
            Revocation information to embed; this should be the output
            of a call to :meth:`.Signer.format_revinfo`
            (ignored when ``use_pades`` is ``True``).
        :param use_pades:
            Respect PAdES requirements.
        :param timestamper:
            :class:`~.timestamps.TimeStamper` used to obtain a trusted timestamp
            token that can be embedded into the signature container.

            .. note::
                If ``dry_run`` is true, the timestamper's
                :meth:`~.timestamps.TimeStamper.dummy_response` method will be
                called to obtain a placeholder token.
                Note that with a standard :class:`~.timestamps.HTTPTimeStamper`,
                this might still hit the timestamping server (in order to
                produce a realistic size estimate), but the dummy response will
                be cached.
        :param cades_signed_attr_meta:
            .. versionadded:: 0.5.0

            Specification for CAdES-specific signed attributes.
        :param encap_content_info:
            Data to encapsulate in the CMS object.

            .. danger::
                This parameter is internal API, and must not be used to produce
                PDF signatures.
        :return:
            An :class:`~.asn1crypto.cms.ContentInfo` object.
        """
        warnings.warn(
            "'Signer.sign' is deprecated, use 'Signer.async_sign' instead",
            DeprecationWarning,
        )
        signed_attr_settings = PdfCMSSignedAttributes(
            signing_time=timestamp,
            adobe_revinfo_attr=revocation_info,
            cades_signed_attrs=cades_signed_attr_meta,
        )
        sign_coro = self.async_sign(
            data_digest=data_digest,
            digest_algorithm=digest_algorithm,
            dry_run=dry_run,
            use_pades=use_pades,
            timestamper=timestamper,
            signed_attr_settings=signed_attr_settings,
            encap_content_info=encap_content_info,
        )
        return asyncio.run(sign_coro)

    def sign_prescribed_attributes(
        self,
        digest_algorithm: str,
        signed_attrs: cms.CMSAttributes,
        cms_version='v1',
        dry_run=False,
        timestamper=None,
        encap_content_info=None,
    ) -> cms.ContentInfo:
        """
        .. versionadded: 0.7.0

        .. deprecated:: 0.9.0
            Use :meth:`async_sign_prescribed_attributes` instead.
            The implementation of this method will invoke
            :meth:`async_sign_prescribed_attributes` using
            ``asyncio.run()``.

        Start the CMS signing process with the prescribed set of signed
        attributes.

        :param digest_algorithm:
            Digest algorithm to use. This should be the same digest method
            as the one used to hash the (external) content.
        :param signed_attrs:
            CMS attributes to sign.
        :param dry_run:
            If ``True``, the actual signing step will be replaced with
            a placeholder.

            In a PDF signing context, this is necessary to estimate the size
            of the signature container before computing the actual digest of
            the document.
        :param timestamper:
            :class:`~.timestamps.TimeStamper` used to obtain a trusted timestamp
            token that can be embedded into the signature container.

            .. note::
                If ``dry_run`` is true, the timestamper's
                :meth:`~.timestamps.TimeStamper.dummy_response` method will be
                called to obtain a placeholder token.
                Note that with a standard :class:`~.timestamps.HTTPTimeStamper`,
                this might still hit the timestamping server (in order to
                produce a realistic size estimate), but the dummy response will
                be cached.
        :param cms_version:
            CMS version to use.
        :param encap_content_info:
            Data to encapsulate in the CMS object.

            .. danger::
                This parameter is internal API, and must not be used to produce
                PDF signatures.
        :return:
            An :class:`~.asn1crypto.cms.ContentInfo` object.
        """
        warnings.warn(
            "'Signer.sign_prescribed_attributes' is deprecated, use "
            "'Signer.async_sign_prescribed_attributes' instead",
            DeprecationWarning,
        )
        sign_coro = self.async_sign_prescribed_attributes(
            digest_algorithm=digest_algorithm,
            signed_attrs=signed_attrs,
            cms_version=cms_version,
            dry_run=dry_run,
            timestamper=timestamper,
            encap_content_info=encap_content_info,
        )
        return asyncio.run(sign_coro)

    def sign_general_data(
        self,
        input_data: Union[
            IO, bytes, cms.ContentInfo, cms.EncapsulatedContentInfo
        ],
        digest_algorithm: str,
        detached=True,
        timestamp: Optional[datetime] = None,
        use_cades=False,
        timestamper=None,
        cades_signed_attr_meta: Optional[CAdESSignedAttrSpec] = None,
        chunk_size=misc.DEFAULT_CHUNK_SIZE,
        max_read=None,
    ) -> cms.ContentInfo:
        """
        .. versionadded:: 0.7.0

        .. deprecated:: 0.9.0
            Use :meth:`async_sign_general_data` instead.
            The implementation of this method will invoke
            :meth:`async_sign_general_data` using ``asyncio.run()``.

        Produce a CMS signature for an arbitrary data stream
        (not necessarily PDF data).


        :param input_data:
            The input data to sign. This can be either a :class:`bytes` object
            a file-type object, a :class:`cms.ContentInfo` object or
            a :class:`cms.EncapsulatedContentInfo` object.

            .. warning::
                ``asn1crypto`` mandates :class:`cms.ContentInfo` for CMS v1
                signatures. In practical terms, this means that you need to
                use :class:`cms.ContentInfo` if the content type is ``data``,
                and :class:`cms.EncapsulatedContentInfo` otherwise.

            .. warning::
                We currently only support CMS v1, v3 and v4 signatures.
                This is only a concern if you need certificates or CRLs
                of type 'other', in which case you can change the version
                yourself (this will not invalidate any signatures).
                You'll also need to do this if you need support for version 1
                attribute certificates, or if you want to sign with
                ``subjectKeyIdentifier`` in the ``sid`` field.
        :param digest_algorithm:
            The name of the digest algorithm to use.
        :param detached:
            If ``True``, create a CMS detached signature (i.e. an object where
            the encapsulated content is not embedded in the signature object
            itself). This is the default. If ``False``, the content to be
            signed will be embedded as encapsulated content.

        :param timestamp:
            Signing time to embed into the signed attributes
            (will be ignored if ``use_cades`` is ``True``).

            .. note::
                This timestamp value is to be interpreted as an unfounded
                assertion by the signer, which may or may not be good enough
                for your purposes.
        :param use_cades:
            Construct a CAdES-style CMS object.
        :param timestamper:
            :class:`.PdfTimeStamper` to use to create a signature timestamp

            .. note::
                If you want to create a *content* timestamp (as opposed to
                a *signature* timestamp), see :class:`.CAdESSignedAttrSpec`.
        :param cades_signed_attr_meta:
            Specification for CAdES-specific signed attributes.
        :param chunk_size:
            Chunk size to use when consuming input data.
        :param max_read:
            Maximal number of bytes to read from the input stream.
        :return:
            A CMS ContentInfo object of type signedData.
        """
        warnings.warn(
            "'Signer.sign_general_data' is deprecated, use "
            "'Signer.async_sign_general_data' instead",
            DeprecationWarning,
        )
        signed_attr_settings = PdfCMSSignedAttributes(
            signing_time=timestamp, cades_signed_attrs=cades_signed_attr_meta
        )
        sign_coro = self.async_sign_general_data(
            input_data=input_data,
            digest_algorithm=digest_algorithm,
            detached=detached,
            use_cades=use_cades,
            timestamper=timestamper,
            chunk_size=chunk_size,
            signed_attr_settings=signed_attr_settings,
            max_read=max_read,
        )
        return asyncio.run(sign_coro)


def asyncify_signer(signer_cls):
    """
    Decorator to turn a legacy :class:`Signer` subclass into one that works
    with the new async API.
    """

    async def async_sign_raw(
        self, data: bytes, digest_algorithm: str, dry_run=False
    ) -> bytes:
        coro = to_thread(
            lambda: signer_cls.sign_raw(
                self,
                data=data,
                digest_algorithm=digest_algorithm,
                dry_run=dry_run,
            )
        )
        return await coro

    signer_cls.async_sign_raw = async_sign_raw
    return signer_cls


class SimpleSigner(Signer):
    """
    Simple signer implementation where the key material is available in local
    memory.
    """

    signing_key: keys.PrivateKeyInfo
    """
    Private key associated with the certificate in :attr:`signing_cert`.
    """

    def __init__(
        self,
        signing_cert: x509.Certificate,
        signing_key: keys.PrivateKeyInfo,
        cert_registry: CertificateStore,
        signature_mechanism: Optional[SignedDigestAlgorithm] = None,
        prefer_pss: bool = False,
        embed_roots: bool = True,
        attribute_certs: Optional[Iterable[cms.AttributeCertificateV2]] = None,
    ):
        self.signing_key = signing_key
        super().__init__(
            prefer_pss=prefer_pss,
            embed_roots=embed_roots,
            cert_registry=cert_registry,
            signature_mechanism=signature_mechanism,
            signing_cert=signing_cert,
        )
        if attribute_certs is not None:
            self._attribute_certs = list(attribute_certs)

    async def async_sign_raw(
        self, data: bytes, digest_algorithm: str, dry_run=False
    ) -> bytes:
        return self.sign_raw(data, digest_algorithm)

    def sign_raw(self, data: bytes, digest_algorithm: str) -> bytes:
        """
        Synchronous raw signature implementation.

        :param data:
            Data to be signed.
        :param digest_algorithm:
            Digest algorithm to use.
        :return:
            Raw signature encoded according to the conventions of the
            signing algorithm used.
        """
        signature_mechanism = self.get_signature_mechanism_for_digest(
            digest_algorithm
        )
        mechanism = signature_mechanism.signature_algo
        priv_key = serialization.load_der_private_key(
            self.signing_key.dump(), password=None
        )

        if mechanism == 'rsassa_pkcs1v15':
            padding = PKCS1v15()
            hash_algo = get_pyca_cryptography_hash(digest_algorithm)
            assert isinstance(priv_key, RSAPrivateKey)
            return priv_key.sign(data, padding, hash_algo)
        elif mechanism == 'rsassa_pss':
            params = signature_mechanism['parameters']
            padding, hash_algo = process_pss_params(params, digest_algorithm)
            assert isinstance(priv_key, RSAPrivateKey)
            return priv_key.sign(data, padding, hash_algo)
        elif mechanism == 'ecdsa':
            hash_algo = get_pyca_cryptography_hash(digest_algorithm)
            assert isinstance(priv_key, EllipticCurvePrivateKey)
            return priv_key.sign(data, signature_algorithm=ECDSA(hash_algo))
        elif mechanism == 'dsa':
            hash_algo = get_pyca_cryptography_hash(digest_algorithm)
            assert isinstance(priv_key, DSAPrivateKey)
            return priv_key.sign(data, hash_algo)
        elif mechanism == 'ed25519':
            assert isinstance(priv_key, Ed25519PrivateKey)
            return priv_key.sign(data)
        elif mechanism == 'ed448':
            assert isinstance(priv_key, Ed448PrivateKey)
            return priv_key.sign(data)
        else:  # pragma: nocover
            raise SigningError(
                f"The signature mechanism {mechanism} "
                "is unsupported by this signer."
            )

    @classmethod
    def _load_ca_chain(cls, ca_chain_files=None):
        try:
            return set(load_certs_from_pemder(ca_chain_files))
        except (IOError, ValueError) as e:  # pragma: nocover
            logger.error('Could not load CA chain', exc_info=e)
            return None

    @classmethod
    def load_pkcs12(
        cls,
        pfx_file,
        ca_chain_files=None,
        other_certs=None,
        passphrase=None,
        signature_mechanism=None,
        prefer_pss=False,
    ):
        """
        Load certificates and key material from a PCKS#12 archive
        (usually ``.pfx`` or ``.p12`` files).

        :param pfx_file:
            Path to the PKCS#12 archive.
        :param ca_chain_files:
            Path to (PEM/DER) files containing other relevant certificates
            not included in the PKCS#12 file.
        :param other_certs:
            Other relevant certificates, specified as a list of
            :class:`.asn1crypto.x509.Certificate` objects.
        :param passphrase:
            Passphrase to decrypt the PKCS#12 archive, if required.
        :param signature_mechanism:
            Override the signature mechanism to use.
        :param prefer_pss:
            Prefer PSS signature mechanism over RSA PKCS#1 v1.5 if
            there's a choice.
        :return:
            A :class:`.SimpleSigner` object initialised with key material loaded
            from the PKCS#12 file provided.
        """
        # TODO support MAC integrity checking?

        try:
            with open(pfx_file, 'rb') as f:
                pfx_bytes = f.read()
        except IOError as e:  # pragma: nocover
            logger.error(f'Could not open PKCS#12 file {pfx_file}.', exc_info=e)
            return None

        ca_chain = (
            cls._load_ca_chain(ca_chain_files) if ca_chain_files else set()
        )
        if ca_chain is None:  # pragma: nocover
            return None
        try:
            (
                private_key,
                cert,
                other_certs_pkcs12,
            ) = pkcs12.load_key_and_certificates(pfx_bytes, passphrase)
        except (IOError, ValueError, TypeError) as e:
            logger.error(
                'Could not load key material from PKCS#12 file', exc_info=e
            )
            return None
        kinfo = _translate_pyca_cryptography_key_to_asn1(private_key)
        cert = _translate_pyca_cryptography_cert_to_asn1(cert)
        other_certs_pkcs12 = set(
            map(_translate_pyca_cryptography_cert_to_asn1, other_certs_pkcs12)
        )

        cs = SimpleCertificateStore()
        certs_to_register = ca_chain | other_certs_pkcs12
        if other_certs is not None:
            certs_to_register |= set(other_certs)
        cs.register_multiple(certs_to_register)
        return SimpleSigner(
            signing_key=kinfo,
            signing_cert=cert,
            cert_registry=cs,
            signature_mechanism=signature_mechanism,
            prefer_pss=prefer_pss,
        )

    @classmethod
    def load(
        cls,
        key_file,
        cert_file,
        ca_chain_files=None,
        key_passphrase=None,
        other_certs=None,
        signature_mechanism=None,
        prefer_pss=False,
    ):
        """
        Load certificates and key material from PEM/DER files.

        :param key_file:
            File containing the signer's private key.
        :param cert_file:
            File containing the signer's certificate.
        :param ca_chain_files:
            File containing other relevant certificates.
        :param key_passphrase:
            Passphrase to decrypt the private key (if required).
        :param other_certs:
            Other relevant certificates, specified as a list of
            :class:`.asn1crypto.x509.Certificate` objects.
        :param signature_mechanism:
            Override the signature mechanism to use.
        :param prefer_pss:
            Prefer PSS signature mechanism over RSA PKCS#1 v1.5 if
            there's a choice.
        :return:
            A :class:`.SimpleSigner` object initialised with key material loaded
            from the files provided.
        """
        try:
            # load cryptographic data (both PEM and DER are supported)
            signing_key = load_private_key_from_pemder(
                key_file, passphrase=key_passphrase
            )
            signing_cert = load_cert_from_pemder(cert_file)
        except (IOError, ValueError, TypeError) as e:
            logger.error('Could not load cryptographic material', exc_info=e)
            return None

        ca_chain = cls._load_ca_chain(ca_chain_files) if ca_chain_files else []
        if ca_chain is None:  # pragma: nocover
            return None

        other_certs = (
            ca_chain if other_certs is None else ca_chain + other_certs
        )

        cert_reg = SimpleCertificateStore()
        cert_reg.register_multiple(other_certs)
        return SimpleSigner(
            signing_cert=signing_cert,
            signing_key=signing_key,
            cert_registry=cert_reg,
            signature_mechanism=signature_mechanism,
            prefer_pss=prefer_pss,
        )


def signer_from_p12_config(
    config: PKCS12SignatureConfig,
    provided_pfx_passphrase: Optional[bytes] = None,
):
    passphrase = config.pfx_passphrase or provided_pfx_passphrase
    result = SimpleSigner.load_pkcs12(
        pfx_file=config.pfx_file,
        passphrase=passphrase,
        other_certs=config.other_certs,
        prefer_pss=config.prefer_pss,
    )
    if result is None:
        raise ConfigurationError("Error while loading key material")
    return result


def signer_from_pemder_config(
    config: PemDerSignatureConfig,
    provided_key_passphrase: Optional[bytes] = None,
):
    key_passphrase = config.key_passphrase or provided_key_passphrase
    result = SimpleSigner.load(
        key_file=config.key_file,
        cert_file=config.cert_file,
        other_certs=config.other_certs,
        prefer_pss=config.prefer_pss,
        key_passphrase=key_passphrase,
    )
    if result is None:
        raise ConfigurationError("Error while loading key material")
    return result


class ExternalSigner(Signer):
    """
    Class to help formatting CMS objects for use with remote signing.
    It embeds a fixed signature value into the CMS, set at initialisation.

    Intended for use with :ref:`interrupted-signing`.

    :param signing_cert:
        The signer's certificate.
    :param cert_registry:
        The certificate registry to use in CMS generation.
    :param signature_value:
        The value of the signature as a byte string, a placeholder length,
        or ``None``.
    :param signature_mechanism:
        The signature mechanism used by the external signing service.
    :param prefer_pss:
        Switch to prefer PSS when producing RSA signatures, as opposed to
        RSA with PKCS#1 v1.5 padding.
    :param embed_roots:
        Whether to embed relevant root certificates into the CMS payload.
    """

    def __init__(
        self,
        signing_cert: Optional[x509.Certificate],
        cert_registry: Optional[CertificateStore],
        signature_value: Union[bytes, int, None] = None,
        signature_mechanism: Optional[SignedDigestAlgorithm] = None,
        prefer_pss: bool = False,
        embed_roots: bool = True,
    ):
        if isinstance(signature_value, bytes):
            self._signature_value = signature_value
        else:
            self._signature_value = bytes(signature_value or 256)
        super().__init__(
            prefer_pss=prefer_pss,
            embed_roots=embed_roots,
            signing_cert=signing_cert,
            cert_registry=cert_registry,
            signature_mechanism=signature_mechanism,
        )

    async def async_sign_raw(
        self, data: bytes, digest_algorithm: str, dry_run=False
    ) -> bytes:
        """
        Return a fixed signature value.
        """
        return self._signature_value


class GenericCMSSignedAttributeProviderSpec(SignedAttributeProviderSpec):
    """
    Signed attribute provider spec for generic CMS signatures.
    """

    def __init__(
        self,
        attr_settings: CMSSignedAttributes,
        signing_cert: Optional[x509.Certificate],
        signature_mechanism: Union[
            Callable[[str], algos.SignedDigestAlgorithm], None
        ],
        timestamper: Optional[TimeStamper],
    ):
        self.signing_cert = signing_cert
        self.attr_settings = attr_settings
        self.signature_mechanism = signature_mechanism
        self.timestamper = timestamper

    def signed_attr_providers(
        self, data_digest: bytes, digest_algorithm: str
    ) -> Iterable[CMSAttributeProvider]:
        attr_settings = self.attr_settings
        if self.signing_cert is not None:
            yield attributes.SigningCertificateV2Provider(
                signing_cert=self.signing_cert
            )
        signing_time = attr_settings.signing_time
        if signing_time is not None:
            yield attributes.SigningTimeProvider(timestamp=signing_time)
        if attr_settings.cades_signed_attrs is not None:
            yield from attr_settings.cades_signed_attrs.prepare_providers(
                message_digest=data_digest,
                md_algorithm=digest_algorithm,
                timestamper=self.timestamper,
            )

        if self.signature_mechanism is not None:
            mech = self.signature_mechanism(digest_algorithm)
            # TODO not sure if PAdES/CAdES allow this, need to check.
            #  It *should*, but perhaps the version of CMS it is based on is too
            #  old, or it might not allow undefined signed attributes.
            # In the meantime, we only add this attribute to non-PAdES sigs
            yield attributes.CMSAlgorithmProtectionProvider(
                digest_algo=digest_algorithm, signature_algo=mech
            )


class GenericPdfSignedAttributeProviderSpec(
    GenericCMSSignedAttributeProviderSpec
):
    """
    Signed attribute provider spec for generic PDF signatures.
    """

    def __init__(
        self,
        attr_settings: PdfCMSSignedAttributes,
        signing_cert: Optional[x509.Certificate],
        signature_mechanism: Union[
            Callable[[str], algos.SignedDigestAlgorithm], None
        ],
        timestamper: Optional[TimeStamper],
    ):
        super().__init__(
            attr_settings=attr_settings,
            signing_cert=signing_cert,
            signature_mechanism=signature_mechanism,
            timestamper=timestamper,
        )

    def signed_attr_providers(
        self, data_digest: bytes, digest_algorithm: str
    ) -> Iterable[CMSAttributeProvider]:
        yield from super().signed_attr_providers(
            data_digest=data_digest, digest_algorithm=digest_algorithm
        )
        attr_settings = self.attr_settings
        assert isinstance(attr_settings, PdfCMSSignedAttributes)
        if attr_settings.adobe_revinfo_attr is not None:
            yield attributes.AdobeRevinfoProvider(
                value=attr_settings.adobe_revinfo_attr
            )


class CAdESSignedAttributeProviderSpec(SignedAttributeProviderSpec):
    """
    Signed attribute provider spec for CAdES and PAdES signatures.
    """

    def __init__(
        self,
        attr_settings: CMSSignedAttributes,
        signing_cert: x509.Certificate,
        is_pades: bool,
        timestamper: Optional[TimeStamper],
    ):
        self.signing_cert = signing_cert
        self.attr_settings = attr_settings
        self.is_pades = is_pades
        self.timestamper = timestamper

    def signed_attr_providers(
        self, data_digest: bytes, digest_algorithm: str
    ) -> Iterable[CMSAttributeProvider]:
        yield attributes.SigningCertificateV2Provider(
            signing_cert=self.signing_cert
        )
        attr_settings = self.attr_settings
        if not self.is_pades:
            # NOTE: PAdES actually forbids this, but CAdES requires it!
            signing_time = attr_settings.signing_time
            if signing_time is None:
                # Ensure CAdES mandate is followed
                signing_time = datetime.now(tz=tzlocal.get_localzone())
            if signing_time is not None:
                yield attributes.SigningTimeProvider(timestamp=signing_time)
        cades_meta = attr_settings.cades_signed_attrs
        if cades_meta is not None:
            yield from cades_meta.prepare_providers(
                message_digest=data_digest,
                md_algorithm=digest_algorithm,
                timestamper=self.timestamper,
            )


class DefaultUnsignedAttributes(UnsignedAttributeProviderSpec):
    """
    Default unsigned attribute provider spec.
    """

    def __init__(self, timestamper: Optional[TimeStamper]):
        self.timestamper = timestamper

    def unsigned_attr_providers(
        self,
        signature: bytes,
        signed_attrs: cms.CMSAttributes,
        digest_algorithm: str,
    ) -> Iterable[CMSAttributeProvider]:
        timestamper = self.timestamper
        if timestamper is not None:
            # the timestamp server needs to cross-sign our signature
            yield attributes.TSTProvider(
                digest_algorithm=digest_algorithm,
                data_to_ts=signature,
                timestamper=timestamper,
            )


RSA_THRESHOLDS = [(2048, 'sha256'), (3072, 'sha384')]
ECC_THRESHOLDS = [(256, 'sha256'), (384, 'sha384')]


def select_suitable_signing_md(key: keys.PublicKeyInfo) -> str:
    """
    Choose a reasonable default signing message digest given the properties of
    (the public part of) a key.

    The fallback value is :const:`constants.DEFAULT_MD`.

    :param key:
        A :class:`keys.PublicKeyInfo` object.
    :return:
        The name of a message digest algorithm.
    """

    def _with_thresholds(key_size, thresholds):
        for sz, md in thresholds:
            if key_size <= sz:
                return md
        return 'sha512'

    key_algo = key.algorithm
    if key_algo == 'rsa':
        return _with_thresholds(key.bit_size, RSA_THRESHOLDS)
    elif key_algo == 'ec':
        return _with_thresholds(key.bit_size, ECC_THRESHOLDS)
    elif key_algo == 'ed25519':
        return 'sha512'
    elif key_algo == 'ed448':
        return 'shake256'
    return constants.DEFAULT_MD
