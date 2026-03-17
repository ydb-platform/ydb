import os
import struct
from typing import Optional

from asn1crypto import cms, tsp
from cryptography.hazmat.primitives import hashes
from pyhanko_certvalidator.registry import CertificateStore

from ..general import get_pyca_cryptography_hash

__all__ = [
    'TimestampRequestError',
    'get_nonce',
    'extract_ts_certs',
    'dummy_digest',
    'handle_tsp_response',
    'set_tsp_headers',
]


class TimestampRequestError(IOError):
    """
    Raised when an error occurs while requesting a timestamp.
    """

    pass


def get_nonce():
    # generate a random 8-byte integer
    # we initialise it like this to guarantee a fixed width
    return struct.unpack('>q', b'\x01' + os.urandom(7))[0]


def extract_ts_certs(ts_token, store: CertificateStore):
    ts_signed_data = ts_token['content']
    ts_certs = ts_signed_data['certificates']

    def extract_ts_sid(si):
        sid = si['sid'].chosen
        # FIXME handle subject key identifier
        assert isinstance(sid, cms.IssuerAndSerialNumber)
        return sid['issuer'].dump(), sid['serial_number'].native

    ts_leaves = set(extract_ts_sid(si) for si in ts_signed_data['signer_infos'])

    for wrapped_c in ts_certs:
        c: cms.Certificate = wrapped_c.chosen
        store.register(c)
        if (c.issuer.dump(), c.serial_number) in ts_leaves:
            yield c


def dummy_digest(md_algorithm: str) -> bytes:
    md_spec = get_pyca_cryptography_hash(md_algorithm)
    return hashes.Hash(md_spec).finalize()


def handle_tsp_response(
    response: tsp.TimeStampResp, nonce: Optional[bytes]
) -> cms.ContentInfo:
    pki_status_info = response['status']
    if pki_status_info['status'].native != 'granted':
        status_strs = pki_status_info['status_string'].native or []
        status_string = '; '.join(status_strs)
        fail_infos = pki_status_info['fail_info'].native or []
        fail_info = '; '.join(fail_infos)
        raise TimestampRequestError(
            f'Timestamp server refused our request: statusString '
            f'\"{status_string}\", failInfo \"{fail_info}\"'
        )
    tst = response['time_stamp_token']
    tst_info = tst['content']['encap_content_info']['content']
    nonce_received = tst_info.parsed['nonce'].native
    if nonce is not None and nonce_received != nonce:
        raise TimestampRequestError(
            f'Time stamping authority sent back bad nonce value. Expected '
            f'{nonce.hex()}, but got {hex(nonce_received)}.'
        )
    return tst


def set_tsp_headers(headers: dict):
    headers['Content-Type'] = 'application/timestamp-query'
    headers['Accept'] = 'application/timestamp-reply'
    return headers
