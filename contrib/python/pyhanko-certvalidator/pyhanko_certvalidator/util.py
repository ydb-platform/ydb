from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import AsyncIterator, Generic, List, Optional, TypeVar, Union

from asn1crypto import algos, cms, core, x509
from asn1crypto.keys import PublicKeyInfo
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import (
    dsa,
    ec,
    ed448,
    ed25519,
    padding,
    rsa,
)


def extract_dir_name(
    names: x509.GeneralNames, err_msg_prefix: str
) -> x509.Name:
    try:
        name: x509.Name = next(
            gname.chosen for gname in names if gname.name == 'directory_name'
        )
    except StopIteration:
        raise NotImplementedError(
            f"{err_msg_prefix}; only distinguished names are supported, "
            f"and none were found."
        )
    return name.untag()


def extract_ac_issuer_dir_name(
    attr_cert: cms.AttributeCertificateV2,
) -> x509.Name:
    issuer_rec = attr_cert['ac_info']['issuer']
    if issuer_rec.name == 'v1_form':
        aa_names = issuer_rec.chosen
    else:
        issuerv2: cms.V2Form = issuer_rec.chosen
        if not isinstance(issuerv2['issuer_name'], core.Void):
            aa_names = issuerv2['issuer_name']
        else:
            aa_names = x509.GeneralNames([])
    return extract_dir_name(aa_names, "Could not extract AC issuer name")


def get_issuer_dn(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
) -> x509.Name:
    if isinstance(cert, x509.Certificate):
        return cert.issuer
    else:
        return extract_ac_issuer_dir_name(cert)


def issuer_serial(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
) -> bytes:
    if isinstance(cert, x509.Certificate):
        return cert.issuer_serial
    else:
        issuer_name = extract_ac_issuer_dir_name(cert)
        result_bytes = b'%s:%d' % (
            issuer_name.sha256,
            cert['ac_info']['serial_number'].native,
        )
        return result_bytes


def get_ac_extension_value(
    attr_cert: cms.AttributeCertificateV2, ext_name: str
):
    try:
        return next(
            ext['extn_value'].parsed
            for ext in attr_cert['ac_info']['extensions']
            if ext['extn_id'].native == ext_name
        )
    except StopIteration:
        return None


def _get_absolute_http_crls(dps: Optional[x509.CRLDistributionPoints]):
    # see x509._get_http_crl_distribution_points

    if dps is None:
        return

    for distribution_point in dps:
        distribution_point_name = distribution_point['distribution_point']
        if isinstance(distribution_point_name, core.Void):
            continue
        # RFC 5280 indicates conforming CA should not use the relative form
        if distribution_point_name.name == 'name_relative_to_crl_issuer':
            continue
        # This library is currently only concerned with HTTP-based CRLs
        for general_name in distribution_point_name.chosen:
            if general_name.name == 'uniform_resource_identifier':
                yield distribution_point


def _get_ac_crl_dps(
    attr_cert: cms.AttributeCertificateV2,
) -> List[x509.DistributionPoint]:
    dps_ext = get_ac_extension_value(attr_cert, 'crl_distribution_points')
    return list(_get_absolute_http_crls(dps_ext))


def _get_ac_delta_crl_dps(
    attr_cert: cms.AttributeCertificateV2,
) -> List[x509.DistributionPoint]:
    delta_dps_ext = get_ac_extension_value(attr_cert, 'freshest_crl')
    return list(_get_absolute_http_crls(delta_dps_ext))


def get_relevant_crl_dps(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2], *, use_deltas
) -> List[x509.DistributionPoint]:
    is_pkc = isinstance(cert, x509.Certificate)

    if is_pkc:
        # FIXME: This utility property in asn1crypto is not precise enough.
        #  More to the point, URLs attached to the same distribution point
        #  are considered interchangeable, but URLs belonging to different
        #  distribution points very much aren't---different distribution points
        #  can differ in what reason codes they record, etc.
        # For the time being, we'll assume that people who care about that sort
        # of nuance will run in 'require' mode, in which case the validator
        # should complain if the available CRLs don't cover all reason codes.
        sources = list(cert.crl_distribution_points)
    else:
        sources = _get_ac_crl_dps(cert)
    if use_deltas:
        if is_pkc:
            sources.extend(cert.delta_crl_distribution_points)
        else:
            sources.extend(_get_ac_delta_crl_dps(cert))

    return sources


def _get_http_ocsp_urls(aia_ext):
    if aia_ext is None:
        return

    for entry in aia_ext:
        # compare x509.Certificate.ocsp_urls
        if entry['access_method'].native == 'ocsp':
            location = entry['access_location']
            if location.name != 'uniform_resource_identifier':
                continue
            url = location.native
            if url.lower().startswith(
                (
                    'http://',
                    'https://',
                )
            ):
                yield url


def get_ocsp_urls(cert: Union[x509.Certificate, cms.AttributeCertificateV2]):
    if isinstance(cert, x509.Certificate):
        aia = cert.authority_information_access_value
    else:
        aia = get_ac_extension_value(cert, 'authority_information_access')

    return list(_get_http_ocsp_urls(aia))


def get_declared_revinfo(
    cert: Union[x509.Certificate, cms.AttributeCertificateV2],
):
    if isinstance(cert, x509.Certificate):
        aia = cert.authority_information_access_value
        crl_dps = cert.crl_distribution_points_value
    else:
        aia = get_ac_extension_value(cert, 'authority_information_access')
        crl_dps = get_ac_extension_value(cert, 'crl_distribution_points')

    has_crl = crl_dps is not None

    # check if the AIA contains any OCSP entries (and here we include all
    #  entries, including those that we can't query)
    if aia is not None:
        has_ocsp = any(entry['access_method'].native == 'ocsp' for entry in aia)
    else:
        has_ocsp = False

    return has_crl, has_ocsp


def validate_sig(
    signature: bytes,
    signed_data: bytes,
    public_key_info: PublicKeyInfo,
    signed_digest_algorithm: algos.SignedDigestAlgorithm,
    parameters=None,
):
    from .errors import DSAParametersUnavailable, PSSParameterMismatch

    sig_algo = signed_digest_algorithm.signature_algo

    if (
        sig_algo == 'dsa'
        and public_key_info['algorithm']['parameters'].native is None
    ):
        raise DSAParametersUnavailable(
            "DSA public key parameters were not provided."
        )

    # pyca/cryptography can't load PSS-exclusive keys without some help:
    if public_key_info.algorithm == 'rsassa_pss':
        public_key_info = public_key_info.copy()
        assert isinstance(parameters, algos.RSASSAPSSParams)
        pss_key_params = public_key_info['algorithm']['parameters'].native
        if pss_key_params is not None and pss_key_params != parameters.native:
            raise PSSParameterMismatch(
                "Public key info includes PSS parameters that do not match "
                "those on the signature"
            )
        # set key type to generic RSA, discard parameters
        public_key_info['algorithm'] = {'algorithm': 'rsa'}

    pub_key = serialization.load_der_public_key(public_key_info.dump())

    if sig_algo == 'rsassa_pkcs1v15':
        hash_algo = signed_digest_algorithm.hash_algo
        assert isinstance(pub_key, rsa.RSAPublicKey)
        h = getattr(hashes, hash_algo.upper())()
        pub_key.verify(signature, signed_data, padding.PKCS1v15(), h)
    elif sig_algo == 'rsassa_pss':
        hash_algo = signed_digest_algorithm.hash_algo
        assert isinstance(pub_key, rsa.RSAPublicKey)
        assert isinstance(parameters, algos.RSASSAPSSParams)
        mga: algos.MaskGenAlgorithm = parameters['mask_gen_algorithm']
        if not mga['algorithm'].native == 'mgf1':
            raise NotImplementedError("Only MFG1 is supported")

        mgf_md_name = mga['parameters']['algorithm'].native

        salt_len: int = parameters['salt_length'].native

        mgf_md = getattr(hashes, mgf_md_name.upper())()
        pss_padding = padding.PSS(
            mgf=padding.MGF1(algorithm=mgf_md), salt_length=salt_len
        )
        hash_spec = getattr(hashes, hash_algo.upper())()
        pub_key.verify(signature, signed_data, pss_padding, hash_spec)
    elif sig_algo == 'dsa':
        hash_algo = signed_digest_algorithm.hash_algo
        assert isinstance(pub_key, dsa.DSAPublicKey)
        hash_spec = getattr(hashes, hash_algo.upper())()
        pub_key.verify(signature, signed_data, hash_spec)
    elif sig_algo == 'ecdsa':
        hash_algo = signed_digest_algorithm.hash_algo
        assert isinstance(pub_key, ec.EllipticCurvePublicKey)
        hash_spec = getattr(hashes, hash_algo.upper())()
        pub_key.verify(signature, signed_data, ec.ECDSA(hash_spec))
    elif sig_algo == 'ed25519':
        assert isinstance(pub_key, ed25519.Ed25519PublicKey)
        pub_key.verify(signature, signed_data)
    elif sig_algo == 'ed448':
        assert isinstance(pub_key, ed448.Ed448PublicKey)
        pub_key.verify(signature, signed_data)
    else:  # pragma: nocover
        raise NotImplementedError(
            f"Signature mechanism {sig_algo} is not supported."
        )


ListElem = TypeVar('ListElem')


@dataclass(frozen=True)
class ConsList(Generic[ListElem]):
    head: Optional[ListElem]
    tail: Optional[ConsList[ListElem]] = None

    @staticmethod
    def empty() -> ConsList[ListElem]:
        return ConsList(head=None)

    @staticmethod
    def sing(value: ListElem) -> ConsList[ListElem]:
        return ConsList(value, ConsList.empty())

    def __iter__(self):
        cur = self
        while cur.head is not None:
            yield cur.head
            cur = cur.tail

    @property
    def last(self) -> Optional[ListElem]:
        cur = self
        result = None
        while cur.tail is not None:
            result = cur.head
            cur = cur.tail
        return result

    def cons(self, head: ListElem) -> ConsList[ListElem]:
        return ConsList(head, self)

    def __repr__(self):  # pragma: nocover
        return f"ConsList({list(reversed(list(self)))})"

    def __bool__(self):
        return self.head is not None


T = TypeVar('T')


class CancelableAsyncIterator(abc.ABC, AsyncIterator[T]):
    async def cancel(self):
        raise NotImplementedError
