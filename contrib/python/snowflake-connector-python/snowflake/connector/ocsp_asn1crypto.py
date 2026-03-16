#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import os
import platform
import sys
import warnings
from base64 import b64decode, b64encode
from collections import OrderedDict
from datetime import datetime, timezone
from logging import getLogger
from os import getenv

from asn1crypto.algos import DigestAlgorithm
from asn1crypto.core import Integer, OctetString
from asn1crypto.ocsp import (
    CertId,
    OCSPRequest,
    OCSPResponse,
    Request,
    Requests,
    TBSRequest,
    Version,
)
from asn1crypto.x509 import Certificate
from Crypto.Hash import SHA1, SHA256, SHA384, SHA512
from Crypto.PublicKey import RSA
from Crypto.Signature import PKCS1_v1_5
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, utils

from snowflake.connector.errorcode import (
    ER_OCSP_RESPONSE_ATTACHED_CERT_EXPIRED,
    ER_OCSP_RESPONSE_ATTACHED_CERT_INVALID,
    ER_OCSP_RESPONSE_CERT_STATUS_INVALID,
    ER_OCSP_RESPONSE_INVALID_SIGNATURE,
    ER_OCSP_RESPONSE_LOAD_FAILURE,
    ER_OCSP_RESPONSE_STATUS_UNSUCCESSFUL,
)
from snowflake.connector.errors import RevocationCheckError
from snowflake.connector.ocsp_snowflake import SnowflakeOCSP, generate_cache_key

with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    # force versioned dylibs onto oscrypto ssl on catalina
    if sys.platform == "darwin" and platform.mac_ver()[0].startswith("10.15"):
        from oscrypto import _module_values, use_openssl

        if _module_values["backend"] is None:
            use_openssl(
                libcrypto_path="/usr/lib/libcrypto.35.dylib",
                libssl_path="/usr/lib/libssl.35.dylib",
            )
    from oscrypto import asymmetric


logger = getLogger(__name__)


class SnowflakeOCSPAsn1Crypto(SnowflakeOCSP):
    """OCSP checks by asn1crypto."""

    # map signature algorithm name to digest class
    SIGNATURE_ALGORITHM_TO_DIGEST_CLASS = {
        "sha256": SHA256,
        "sha384": SHA384,
        "sha512": SHA512,
    }

    SIGNATURE_ALGORITHM_TO_DIGEST_CLASS_OPENSSL = {
        "sha256": hashes.SHA256,
        "sha384": hashes.SHA3_384,
        "sha512": hashes.SHA3_512,
    }

    def encode_cert_id_key(self, hkey):
        issuer_name_hash, issuer_key_hash, serial_number = hkey
        issuer_name_hash = OctetString.load(issuer_name_hash)
        issuer_key_hash = OctetString.load(issuer_key_hash)
        serial_number = Integer.load(serial_number)
        cert_id = CertId(
            {
                "hash_algorithm": DigestAlgorithm(
                    {"algorithm": "sha1", "parameters": None}
                ),
                "issuer_name_hash": issuer_name_hash,
                "issuer_key_hash": issuer_key_hash,
                "serial_number": serial_number,
            }
        )
        return cert_id

    def decode_cert_id_key(self, cert_id: CertId) -> tuple[bytes, bytes, bytes]:
        return generate_cache_key(cert_id)

    def decode_cert_id_base64(self, cert_id_base64):
        return CertId.load(b64decode(cert_id_base64))

    def encode_cert_id_base64(self, hkey):
        return b64encode(self.encode_cert_id_key(hkey).dump()).decode("ascii")

    def read_cert_bundle(self, ca_bundle_file, storage=None):
        """Reads a certificate file including certificates in PEM format."""
        if storage is None:
            storage = SnowflakeOCSP.ROOT_CERTIFICATES_DICT
        logger.debug("reading certificate bundle: %s", ca_bundle_file)
        with open(ca_bundle_file, "rb") as all_certs:
            # don't lock storage
            from asn1crypto import pem

            pem_certs = pem.unarmor(all_certs.read(), multiple=True)
            for type_name, _, der_bytes in pem_certs:
                if type_name == "CERTIFICATE":
                    crt = Certificate.load(der_bytes)
                    storage[crt.subject.sha256] = crt

    def create_ocsp_request(
        self,
        issuer: Certificate,
        subject: Certificate,
    ) -> tuple[CertId, OCSPRequest]:
        """Creates CertId and OCSPRequest."""
        cert_id = CertId(
            {
                "hash_algorithm": DigestAlgorithm(
                    {"algorithm": "sha1", "parameters": None}
                ),
                "issuer_name_hash": OctetString(subject.issuer.sha1),
                "issuer_key_hash": OctetString(issuer.public_key.sha1),
                "serial_number": subject.serial_number,
            }
        )
        ocsp_request = OCSPRequest(
            {
                "tbs_request": TBSRequest(
                    {
                        "version": Version(0),
                        "request_list": Requests(
                            [
                                Request(
                                    {
                                        "req_cert": cert_id,
                                    }
                                )
                            ]
                        ),
                    }
                ),
            }
        )
        return cert_id, ocsp_request

    def extract_ocsp_url(self, cert):
        urls = cert.ocsp_urls
        ocsp_url = urls[0] if urls else None
        return ocsp_url

    def decode_ocsp_request(self, ocsp_request):
        return ocsp_request.dump()

    def decode_ocsp_request_b64(self, ocsp_request):
        data = self.decode_ocsp_request(ocsp_request)  # convert to DER
        b64data = b64encode(data).decode("ascii")
        return b64data

    def extract_good_status(self, single_response):
        """Extracts GOOD status."""
        this_update_native = single_response["this_update"].native
        next_update_native = single_response["next_update"].native

        return this_update_native, next_update_native

    def extract_revoked_status(self, single_response):
        """Extracts REVOKED status."""
        revoked_info = single_response["cert_status"]
        revocation_time = revoked_info.native["revocation_time"]
        revocation_reason = revoked_info.native["revocation_reason"]
        return revocation_time, revocation_reason

    def check_cert_time_validity(self, cur_time, ocsp_cert):

        val_start = ocsp_cert["tbs_certificate"]["validity"]["not_before"].native
        val_end = ocsp_cert["tbs_certificate"]["validity"]["not_after"].native

        if cur_time > val_end or cur_time < val_start:
            debug_msg = (
                "Certificate attached to OCSP response is invalid. OCSP response "
                "current time - {} certificate not before time - {} certificate "
                "not after time - {}. Consider running curl -o ocsp.der {}".format(
                    cur_time,
                    val_start,
                    val_end,
                    super().debug_ocsp_failure_url,
                )
            )

            return False, debug_msg
        else:
            return True, None

    """
    is_valid_time - checks various components of the OCSP Response
    for expiry.
    :param cert_id - certificate id corresponding to OCSP Response
    :param ocsp_response
    :return True/False depending on time validity within the response
    """

    def is_valid_time(self, cert_id, ocsp_response):
        res = OCSPResponse.load(ocsp_response)

        if res["response_status"].native != "successful":
            raise RevocationCheckError(
                msg="Invalid Status: {}".format(res["response_status"].native),
                errno=ER_OCSP_RESPONSE_STATUS_UNSUCCESSFUL,
            )

        basic_ocsp_response = res.basic_ocsp_response
        if basic_ocsp_response["certs"].native:
            ocsp_cert = basic_ocsp_response["certs"][0]
            logger.debug(
                "Verifying the attached certificate is signed by "
                "the issuer. Valid Not After: %s",
                ocsp_cert["tbs_certificate"]["validity"]["not_after"].native,
            )

            cur_time = datetime.now(timezone.utc)

            """
            Note:
            We purposefully do not verify certificate signature here.
            The OCSP Response is extracted from the OCSP Response Cache
            which is expected to have OCSP Responses with verified
            attached signature. Moreover this OCSP Response is eventually
            going to be processed by the driver before being consumed by
            the driver.
            This step ensures that the OCSP Response cache does not have
            any invalid entries.
            """
            cert_valid, debug_msg = self.check_cert_time_validity(cur_time, ocsp_cert)
            if not cert_valid:
                logger.debug(debug_msg)
                return False

        tbs_response_data = basic_ocsp_response["tbs_response_data"]

        single_response = tbs_response_data["responses"][0]
        cert_status = single_response["cert_status"].name

        try:
            if cert_status == "good":
                self._process_good_status(single_response, cert_id, ocsp_response)
        except Exception as ex:
            logger.debug("Failed to validate ocsp response %s", ex)
            return False

        return True

    def process_ocsp_response(self, issuer, cert_id, ocsp_response):
        try:
            res = OCSPResponse.load(ocsp_response)
            if self.test_mode is not None:
                ocsp_load_failure = getenv("SF_TEST_OCSP_FORCE_BAD_OCSP_RESPONSE")
                if ocsp_load_failure is not None:
                    raise RevocationCheckError(
                        "Force fail", errno=ER_OCSP_RESPONSE_LOAD_FAILURE
                    )
        except Exception:
            raise RevocationCheckError(
                msg="Invalid OCSP Response", errno=ER_OCSP_RESPONSE_LOAD_FAILURE
            )

        if res["response_status"].native != "successful":
            raise RevocationCheckError(
                msg="Invalid Status: {}".format(res["response_status"].native),
                errno=ER_OCSP_RESPONSE_STATUS_UNSUCCESSFUL,
            )

        basic_ocsp_response = res.basic_ocsp_response
        if basic_ocsp_response["certs"].native:
            logger.debug("Certificate is attached in Basic OCSP Response")
            ocsp_cert = basic_ocsp_response["certs"][0]
            logger.debug(
                "Verifying the attached certificate is signed by " "the issuer"
            )
            logger.debug(
                "Valid Not After: %s",
                ocsp_cert["tbs_certificate"]["validity"]["not_after"].native,
            )

            cur_time = datetime.now(timezone.utc)

            try:
                """
                Signature verification should happen before any kind of
                validation
                """
                self.verify_signature(
                    ocsp_cert.hash_algo,
                    ocsp_cert.signature,
                    issuer,
                    ocsp_cert["tbs_certificate"],
                )
            except RevocationCheckError as rce:
                raise RevocationCheckError(
                    msg=rce.msg, errno=ER_OCSP_RESPONSE_ATTACHED_CERT_INVALID
                )
            cert_valid, debug_msg = self.check_cert_time_validity(cur_time, ocsp_cert)

            if not cert_valid:
                raise RevocationCheckError(
                    msg=debug_msg, errno=ER_OCSP_RESPONSE_ATTACHED_CERT_EXPIRED
                )

        else:
            logger.debug(
                "Certificate is NOT attached in Basic OCSP Response. "
                "Using issuer's certificate"
            )
            ocsp_cert = issuer

        tbs_response_data = basic_ocsp_response["tbs_response_data"]

        logger.debug("Verifying the OCSP response is signed by the issuer.")
        try:
            self.verify_signature(
                basic_ocsp_response["signature_algorithm"].hash_algo,
                basic_ocsp_response["signature"].native,
                ocsp_cert,
                tbs_response_data,
            )
        except RevocationCheckError as rce:
            raise RevocationCheckError(
                msg=rce.msg, errno=ER_OCSP_RESPONSE_INVALID_SIGNATURE
            )

        single_response = tbs_response_data["responses"][0]
        cert_status = single_response["cert_status"].name
        if self.test_mode is not None:
            test_cert_status = getenv("SF_TEST_OCSP_CERT_STATUS")
            if test_cert_status == "revoked":
                cert_status = "revoked"
            elif test_cert_status == "unknown":
                cert_status = "unknown"
            elif test_cert_status == "good":
                cert_status = "good"

        try:
            if cert_status == "good":
                self._process_good_status(single_response, cert_id, ocsp_response)
            elif cert_status == "revoked":
                self._process_revoked_status(single_response, cert_id)
            elif cert_status == "unknown":
                self._process_unknown_status(cert_id)
            else:
                debug_msg = (
                    "Unknown revocation status was returned."
                    "OCSP response may be malformed: {}.".format(cert_status)
                )
                raise RevocationCheckError(
                    msg=debug_msg, errno=ER_OCSP_RESPONSE_CERT_STATUS_INVALID
                )
        except RevocationCheckError as op_er:
            debug_msg = "{} Consider running curl -o ocsp.der {}".format(
                op_er.msg, self.debug_ocsp_failure_url
            )
            raise RevocationCheckError(msg=debug_msg, errno=op_er.errno)

    def verify_signature(self, signature_algorithm, signature, cert, data):
        use_openssl_only = os.getenv("SF_USE_OPENSSL_ONLY", "False") == "True"
        if not use_openssl_only:
            pubkey = asymmetric.load_public_key(cert.public_key).unwrap().dump()
            rsakey = RSA.importKey(pubkey)
            signer = PKCS1_v1_5.new(rsakey)
            if (
                signature_algorithm
                in SnowflakeOCSPAsn1Crypto.SIGNATURE_ALGORITHM_TO_DIGEST_CLASS
            ):
                digest = SnowflakeOCSPAsn1Crypto.SIGNATURE_ALGORITHM_TO_DIGEST_CLASS[
                    signature_algorithm
                ].new()
            else:
                # the last resort. should not happen.
                digest = SHA1.new()
            digest.update(data.dump())
            if not signer.verify(digest, signature):
                raise RevocationCheckError(msg="Failed to verify the signature")

        else:
            backend = default_backend()
            public_key = serialization.load_der_public_key(
                cert.public_key.dump(), backend=default_backend()
            )
            if (
                signature_algorithm
                in SnowflakeOCSPAsn1Crypto.SIGNATURE_ALGORITHM_TO_DIGEST_CLASS
            ):
                chosen_hash = (
                    SnowflakeOCSPAsn1Crypto.SIGNATURE_ALGORITHM_TO_DIGEST_CLASS_OPENSSL[
                        signature_algorithm
                    ]()
                )
            else:
                # the last resort. should not happen.
                chosen_hash = hashes.SHA1()
            hasher = hashes.Hash(chosen_hash, backend)
            hasher.update(data.dump())
            digest = hasher.finalize()
            try:
                public_key.verify(
                    signature, digest, padding.PKCS1v15(), utils.Prehashed(chosen_hash)
                )
            except InvalidSignature:
                raise RevocationCheckError(msg="Failed to verify the signature")

    def extract_certificate_chain(self, connection):
        """Gets certificate chain and extract the key info from OpenSSL connection."""
        from OpenSSL.crypto import FILETYPE_ASN1, dump_certificate

        cert_map = OrderedDict()
        logger.debug("# of certificates: %s", len(connection.get_peer_cert_chain()))

        for cert_openssl in connection.get_peer_cert_chain():
            cert_der = dump_certificate(FILETYPE_ASN1, cert_openssl)
            cert = Certificate.load(cert_der)
            logger.debug(
                "subject: %s, issuer: %s", cert.subject.native, cert.issuer.native
            )
            cert_map[cert.subject.sha256] = cert

        return self.create_pair_issuer_subject(cert_map)

    def create_pair_issuer_subject(self, cert_map):
        """Creates pairs of issuer and subject certificates."""
        issuer_subject = []
        for subject_der in cert_map:
            subject = cert_map[subject_der]
            if subject.ocsp_no_check_value or subject.ca and not subject.ocsp_urls:
                # Root certificate will not be validated
                # but it is used to validate the subject certificate
                continue
            issuer_hash = subject.issuer.sha256
            if issuer_hash not in cert_map:
                # IF NO ROOT certificate is attached in the certificate chain
                # read it from the local disk
                self._lazy_read_ca_bundle()
                logger.debug("not found issuer_der: %s", subject.issuer.native)
                if issuer_hash not in SnowflakeOCSP.ROOT_CERTIFICATES_DICT:
                    raise RevocationCheckError(
                        msg="CA certificate is NOT found in the root "
                        "certificate list. Make sure you use the latest "
                        "Python Connector package and the URL is valid."
                    )
                issuer = SnowflakeOCSP.ROOT_CERTIFICATES_DICT[issuer_hash]
            else:
                issuer = cert_map[issuer_hash]

            issuer_subject.append((issuer, subject))
        return issuer_subject

    def subject_name(self, subject):
        return subject.subject.native
