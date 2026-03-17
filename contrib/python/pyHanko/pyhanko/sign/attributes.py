import abc
from datetime import datetime
from typing import Iterable, Optional

from asn1crypto import algos, cms, core
from asn1crypto import pdf as asn1_pdf
from asn1crypto import tsp, x509
from cryptography.hazmat.primitives import hashes

from .general import (
    as_signing_certificate_v2,
    get_pyca_cryptography_hash,
    simple_cms_attribute,
)
from .timestamps import TimeStamper

__all__ = [
    'SignedAttributeProviderSpec',
    'UnsignedAttributeProviderSpec',
    'CMSAttributeProvider',
    'SigningTimeProvider',
    'SigningCertificateV2Provider',
    'AdobeRevinfoProvider',
    'CMSAlgorithmProtectionProvider',
    'TSTProvider',
]


class CMSAttributeProvider:
    """
    Base class to provide asynchronous CMS attribute values.
    """

    attribute_type: str
    """
    Name of the CMS attribute type this provider supplies. See
    :class:`cms.CMSAttributeType`.
    """

    async def build_attr_value(self, dry_run=False):
        """
        Build the attribute value asynchronously.

        :param dry_run:
            ``True`` if the signer is operating in dry-run (size estimation)
            mode.
        :return:
            An attribute value appropriate for the attribute type.
        """
        raise NotImplementedError

    async def get_attribute(self, dry_run=False) -> Optional[cms.CMSAttribute]:
        value = await self.build_attr_value(dry_run=dry_run)
        if value is not None:
            return simple_cms_attribute(self.attribute_type, value)
        else:
            return None


class SigningCertificateV2Provider(CMSAttributeProvider):
    """
    Provide a value for the signing-certificate-v2 attribute.

    :param signing_cert:
        Certificate containing the signer's public key.
    """

    attribute_type: str = 'signing_certificate_v2'

    def __init__(self, signing_cert: x509.Certificate):
        self.signing_cert = signing_cert

    async def build_attr_value(self, dry_run=False) -> tsp.SigningCertificateV2:
        return as_signing_certificate_v2(self.signing_cert)


class SigningTimeProvider(CMSAttributeProvider):
    """
    Provide a value for the signing-time attribute (i.e. an otherwise
    unauthenticated timestamp).

    :param timestamp:
        Datetime object to include.
    """

    attribute_type: str = 'signing_time'

    def __init__(self, timestamp: datetime):
        self.timestamp = timestamp

    async def build_attr_value(self, dry_run=False) -> cms.Time:
        return cms.Time({'utc_time': core.UTCTime(self.timestamp)})


# TODO: would be nice to also provide an implementation that does this with
#  a "live" revocation checker, for use in the lower-level APIs
# This one relies on the revinfo being available from earlier preprocessing
# steps, which is good enough for internal use.


class AdobeRevinfoProvider(CMSAttributeProvider):
    """
    Yield Adobe-style revocation information for inclusion into a CMS
    object.

    :param value:
        A (pre-formatted) RevocationInfoArchival object.
    """

    attribute_type: str = 'adobe_revocation_info_archival'

    def __init__(self, value: asn1_pdf.RevocationInfoArchival):
        self.value = value

    async def build_attr_value(
        self, dry_run=False
    ) -> Optional[asn1_pdf.RevocationInfoArchival]:
        return self.value


class CMSAlgorithmProtectionProvider(CMSAttributeProvider):
    attribute_type: str = 'cms_algorithm_protection'

    def __init__(
        self, digest_algo: str, signature_algo: algos.SignedDigestAlgorithm
    ):
        self.digest_algo = digest_algo
        self.signature_algo = signature_algo

    async def build_attr_value(
        self, dry_run=False
    ) -> cms.CMSAlgorithmProtection:
        digest_algorithm_args = {'algorithm': self.digest_algo}
        if self.digest_algo == 'shake256':
            # RFC 8419 requirement
            mech = self.signature_algo
            if mech['algorithm'].native == 'ed448':
                digest_algorithm_args = {
                    'algorithm': 'shake256_len',
                    'parameters': core.Integer(512),
                }
        return cms.CMSAlgorithmProtection(
            {
                'digest_algorithm': algos.DigestAlgorithm(
                    digest_algorithm_args
                ),
                'signature_algorithm': self.signature_algo,
            }
        )


class TSTProvider(CMSAttributeProvider):
    def __init__(
        self,
        digest_algorithm: str,
        data_to_ts: bytes,
        timestamper: TimeStamper,
        attr_type: str = 'signature_time_stamp_token',
        prehashed=False,
    ):
        self.attribute_type = attr_type
        self.digest_algorithm = digest_algorithm
        self.timestamper = timestamper
        self.data = data_to_ts
        self.prehashed = prehashed

    async def build_attr_value(self, dry_run=False) -> cms.ContentInfo:
        digest_algorithm = self.digest_algorithm
        if self.prehashed:
            digest = self.data
        else:
            md_spec = get_pyca_cryptography_hash(digest_algorithm)
            md = hashes.Hash(md_spec)
            md.update(self.data)
            digest = md.finalize()
        if dry_run:
            ts_coro = self.timestamper.async_dummy_response(digest_algorithm)
        else:
            ts_coro = self.timestamper.async_timestamp(digest, digest_algorithm)
        return await ts_coro


class SignedAttributeProviderSpec(abc.ABC):
    """
    .. versionadded:: 0.14.0

    Interface for setting up signed attributes, independently of the
    :class:`~pyhanko.sign.signers.pdf_cms.Signer` hierarchy.
    """

    def signed_attr_providers(
        self, data_digest: bytes, digest_algorithm: str
    ) -> Iterable[CMSAttributeProvider]:
        """
        Lazily set up signed attribute providers.

        :param data_digest:
            The digest of the data to be signed.
        :param digest_algorithm:
            The digest algorithm used.
        """
        raise NotImplementedError


class UnsignedAttributeProviderSpec(abc.ABC):
    """
    .. versionadded:: 0.14.0

    Interface for setting up unsigned attributes, independently of the
    :class:`~pyhanko.sign.signers.pdf_cms.Signer` hierarchy.
    """

    def unsigned_attr_providers(
        self,
        signature: bytes,
        signed_attrs: cms.CMSAttributes,
        digest_algorithm: str,
    ) -> Iterable[CMSAttributeProvider]:
        """
        Lazily set up unsigned attribute providers.

        :param signature:
            The signature computed over the signed attributes.
        :param signed_attrs:
            Signed attributes over which the signature was taken.
        :param digest_algorithm:
            The digest algorithm used.
        """
        raise NotImplementedError
