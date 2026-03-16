from datetime import datetime
from typing import Optional

import tzlocal
from asn1crypto import algos, cms, core, keys, tsp, x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.padding import PKCS1v15
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from pyhanko_certvalidator.registry import CertificateStore

from .. import general
from ..general import get_pyca_cryptography_hash, simple_cms_attribute
from .api import TimeStamper
from .common_utils import get_nonce

__all__ = ['DummyTimeStamper']


class DummyTimeStamper(TimeStamper):
    """
    Timestamper that acts as its own TSA. It accepts all requests and
    signs them using the certificate provided.
    Used for testing purposes.
    """

    def __init__(
        self,
        tsa_cert: x509.Certificate,
        tsa_key: keys.PrivateKeyInfo,
        certs_to_embed: Optional[CertificateStore] = None,
        fixed_dt: Optional[datetime] = None,
        include_nonce=True,
        override_md=None,
    ):
        self.tsa_cert = tsa_cert
        self.tsa_key = tsa_key
        self.certs_to_embed = list(certs_to_embed or ())
        self.fixed_dt = fixed_dt
        self.override_md = override_md
        super().__init__(include_nonce=include_nonce)

    def request_tsa_response(self, req: tsp.TimeStampReq) -> tsp.TimeStampResp:
        # We pretend that certReq is always true in the request

        # TODO generalise my detached signature logic to include cases like this
        #  (see § 5.4 in RFC 5652)
        # TODO does the RFC
        status = tsp.PKIStatusInfo({'status': tsp.PKIStatus('granted')})
        message_imprint: tsp.MessageImprint = req['message_imprint']
        md_algorithm = self.override_md
        if md_algorithm is None:
            md_algorithm = message_imprint['hash_algorithm']['algorithm'].native
        digest_algorithm_obj = algos.DigestAlgorithm(
            {'algorithm': md_algorithm}
        )
        dt = self.fixed_dt or datetime.now(tz=tzlocal.get_localzone())
        tst_info_args = {
            'version': 'v1',
            # See http://oidref.com/1.3.6.1.4.1.4146.2.2
            # I don't really care too much, this is a testing device anyway
            'policy': tsp.ObjectIdentifier('1.3.6.1.4.1.4146.2.2'),
            'message_imprint': message_imprint,
            # should be sufficiently random (again, this is a testing class)
            'serial_number': get_nonce(),
            'gen_time': dt,
            'tsa': x509.GeneralName(
                name='directory_name', value=self.tsa_cert.subject
            ),
        }
        if req['nonce'].native is not None:
            tst_info_args['nonce'] = req['nonce']

        tst_info = tsp.TSTInfo(tst_info_args)
        tst_info_data = tst_info.dump()
        md_spec = get_pyca_cryptography_hash(md_algorithm)
        md = hashes.Hash(md_spec)
        md.update(tst_info_data)
        message_digest_value = md.finalize()
        signed_attrs = cms.CMSAttributes(
            [
                simple_cms_attribute('content_type', 'tst_info'),
                simple_cms_attribute(
                    'signing_time', cms.Time({'utc_time': core.UTCTime(dt)})
                ),
                simple_cms_attribute(
                    'signing_certificate',
                    general.as_signing_certificate(self.tsa_cert),
                ),
                simple_cms_attribute('message_digest', message_digest_value),
            ]
        )
        priv_key = serialization.load_der_private_key(
            self.tsa_key.dump(), password=None
        )
        if not isinstance(priv_key, RSAPrivateKey):
            raise NotImplementedError("Dummy timestamper is RSA-only.")
        signature = priv_key.sign(
            signed_attrs.dump(),
            PKCS1v15(),
            get_pyca_cryptography_hash(md_algorithm.upper()),
        )
        sig_info = cms.SignerInfo(
            {
                'version': 'v1',
                'sid': cms.SignerIdentifier(
                    {
                        'issuer_and_serial_number': cms.IssuerAndSerialNumber(
                            {
                                'issuer': self.tsa_cert.issuer,
                                'serial_number': self.tsa_cert.serial_number,
                            }
                        )
                    }
                ),
                'digest_algorithm': digest_algorithm_obj,
                'signature_algorithm': algos.SignedDigestAlgorithm(
                    {'algorithm': 'rsassa_pkcs1v15'}
                ),
                'signed_attrs': signed_attrs,
                'signature': signature,
            }
        )
        certs = set(self.certs_to_embed)
        certs.add(self.tsa_cert)
        signed_data = {
            # must use v3 to get access to the EncapsulatedContentInfo construct
            'version': 'v3',
            'digest_algorithms': cms.DigestAlgorithms((digest_algorithm_obj,)),
            'encap_content_info': cms.EncapsulatedContentInfo(
                {
                    'content_type': cms.ContentType('tst_info'),
                    'content': cms.ParsableOctetString(tst_info_data),
                }
            ),
            'certificates': certs,
            'signer_infos': [sig_info],
        }
        tst = cms.ContentInfo(
            {
                'content_type': cms.ContentType('signed_data'),
                'content': cms.SignedData(signed_data),
            }
        )
        return tsp.TimeStampResp({'status': status, 'time_stamp_token': tst})

    async def async_request_tsa_response(
        self, req: tsp.TimeStampReq
    ) -> tsp.TimeStampResp:
        # just do it synchronously
        return self.request_tsa_response(req)
