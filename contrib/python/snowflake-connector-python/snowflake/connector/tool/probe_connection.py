#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from socket import gaierror, gethostbyname_ex

from asn1crypto import ocsp
from OpenSSL.crypto import FILETYPE_ASN1, dump_certificate

from ..compat import urlsplit
from ..ssl_wrap_socket import _openssl_connect


def probe_connection(url):
    parsed_url = urlsplit(url)

    # DNS lookup
    try:
        actual_hostname, aliases, ips = gethostbyname_ex(parsed_url.hostname)
        ret = {
            "url": url,
            "input_hostname": parsed_url.hostname,
            "actual_hostname": actual_hostname,
            "aliases": aliases,
            "ips": ips,
        }
    except gaierror as e:
        return {"err:": e}
    connection = _openssl_connect(parsed_url.hostname, parsed_url.port)

    # certificates
    certificates = []
    for cert_openssl in connection.get_peer_cert_chain():
        cert_der = dump_certificate(FILETYPE_ASN1, cert_openssl)
        cert = ocsp.Certificate.load(cert_der)
        ocsp_uris = cert.ocsp_urls

        if len(ocsp_uris) == 1:
            parsed_ocsp_url = urlsplit(ocsp_uris[0])

            # DNS lookup for OCSP server
            try:
                actual_hostname, aliases, ips = gethostbyname_ex(
                    parsed_ocsp_url.hostname
                )
                ocsp_status = {
                    "input_url": ocsp_uris[0],
                    "actual_hostname": actual_hostname,
                    "aliases": aliases,
                    "ips": ips,
                }
            except gaierror as e:
                ocsp_status = {
                    "input_url": ocsp_uris[0],
                    "error": e,
                }
        else:
            ocsp_status = {}

        certificates.append(
            {
                "hash": cert.subject.sha1,
                "name": cert.subject.native,
                "issuer": cert.issuer.native,
                "serial_number": cert.serial_number,
                "ocsp": ocsp_status,
            }
        )

    ret["certificates"] = certificates
    return ret
