#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import json
import sys
from datetime import datetime
from glob import glob
from os import path
from time import gmtime, strftime, time

from asn1crypto import core, ocsp
from asn1crypto.x509 import Certificate
from OpenSSL.crypto import FILETYPE_ASN1, dump_certificate

from snowflake.connector.ocsp_asn1crypto import SnowflakeOCSPAsn1Crypto as SFOCSP
from snowflake.connector.ssl_wrap_socket import _openssl_connect

ZERO_EPOCH = datetime.utcfromtimestamp(0)

OCSP_CACHE_SERVER_INTERVAL = 20 * 60 * 60  # seconds


def main():
    """Internal Tool: Dump OCSP response cache file."""

    def help():
        print(
            "Dump OCSP Response cache. This tools extracts OCSP response "
            "cache file, i.e., ~/.cache/snowflake/ocsp_response_cache. "
            "Note the subject name shows up if the certificate exists in "
            "the certs directory."
        )
        print(
            """
Usage: {}  <ocsp response cache file> <hostname file> <cert file glob pattern>
""".format(
                path.basename(sys.argv[0])
            )
        )
        sys.exit(2)

    if len(sys.argv) < 4:
        help()
        sys.exit(2)

    ocsp_response_cache_file = sys.argv[1]
    if not path.isfile(ocsp_response_cache_file):
        help()
        sys.exit(2)

    hostname_file = sys.argv[2]
    cert_glob_pattern = sys.argv[3]
    dump_ocsp_response_cache(ocsp_response_cache_file, hostname_file, cert_glob_pattern)


def raise_old_cache_exception(current_time, created_on, name, serial_number):
    raise Exception(
        "ERROR: OCSP response cache is too old. created_on "
        "should be newer than {}: "
        "name: {}, serial_number: {}, "
        "current_time: {}, created_on: {}".format(
            strftime(
                SFOCSP.OUTPUT_TIMESTAMP_FORMAT,
                gmtime(current_time - OCSP_CACHE_SERVER_INTERVAL),
            ),
            name,
            serial_number,
            strftime(SFOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(current_time)),
            strftime(SFOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(created_on)),
        )
    )


def raise_outdated_validity_exception(
    current_time, name, serial_number, this_update, next_update
):
    raise Exception(
        "ERROR: OCSP response cache include "
        "outdated data: "
        "name: {}, serial_number: {}, "
        "current_time: {}, this_update: {}, "
        "next_update: {}".format(
            name,
            serial_number,
            strftime(SFOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(current_time)),
            this_update.strftime(SFOCSP.OUTPUT_TIMESTAMP_FORMAT),
            next_update.strftime(SFOCSP.OUTPUT_TIMESTAMP_FORMAT),
        )
    )


def dump_ocsp_response_cache(
    ocsp_response_cache_file, hostname_file, cert_glob_pattern
):
    """Dump OCSP response cache contents.

    Show the subject name as well if the subject is included in the certificate files.
    """
    sfocsp = SFOCSP()
    s_to_n = _fetch_certs(hostname_file)
    s_to_n1 = _serial_to_name(sfocsp, cert_glob_pattern)
    s_to_n.update(s_to_n1)

    SFOCSP.OCSP_CACHE.read_ocsp_response_cache_file(sfocsp, ocsp_response_cache_file)

    def custom_key(k):
        # third element is Serial Number for the subject
        serial_number = core.Integer.load(k[2])
        return int(serial_number.native)

    output = {}
    ocsp_validation_cache = SFOCSP.OCSP_CACHE.CACHE
    for hkey in sorted(ocsp_validation_cache, key=custom_key):
        json_key = sfocsp.encode_cert_id_base64(hkey)

        serial_number = core.Integer.load(hkey[2]).native
        if int(serial_number) in s_to_n:
            name = s_to_n[int(serial_number)]
        else:
            name = "Unknown"
        output[json_key] = {
            "serial_number": format(serial_number, "d"),
            "name": name,
        }
        value = ocsp_validation_cache[hkey]
        cache = value[1]
        ocsp_response = ocsp.OCSPResponse.load(cache)
        basic_ocsp_response = ocsp_response.basic_ocsp_response

        tbs_response_data = basic_ocsp_response["tbs_response_data"]

        current_time = int(time())
        for single_response in tbs_response_data["responses"]:
            created_on = int(value[0])
            produce_at = tbs_response_data["produced_at"].native
            this_update = single_response["this_update"].native
            next_update = single_response["next_update"].native
            if current_time - OCSP_CACHE_SERVER_INTERVAL > created_on:
                raise_old_cache_exception(current_time, created_on, name, serial_number)

            next_update_utc = (
                next_update.replace(tzinfo=None) - ZERO_EPOCH
            ).total_seconds()
            this_update_utc = (
                this_update.replace(tzinfo=None) - ZERO_EPOCH
            ).total_seconds()

            if current_time > next_update_utc or current_time < this_update_utc:
                raise_outdated_validity_exception(
                    current_time, name, serial_number, this_update, next_update
                )

            output[json_key]["created_on"] = strftime(
                SFOCSP.OUTPUT_TIMESTAMP_FORMAT, gmtime(created_on)
            )
            output[json_key]["produce_at"] = str(produce_at)
            output[json_key]["this_update"] = str(this_update)
            output[json_key]["next_update"] = str(next_update)
    print(json.dumps(output))


def _serial_to_name(sfocsp, cert_glob_pattern):
    """Creates a map table from serial number to name."""
    map_serial_to_name = {}
    for cert_file in glob(cert_glob_pattern):
        cert_map = {}
        sfocsp.read_cert_bundle(cert_file, cert_map)
        cert_data = sfocsp.create_pair_issuer_subject(cert_map)

        for _, subject in cert_data:
            map_serial_to_name[subject.serial_number] = subject.subject.native

    return map_serial_to_name


def _fetch_certs(hostname_file):
    with open(hostname_file) as f:
        hostnames = f.read().split("\n")

    map_serial_to_name = {}
    for h in hostnames:
        if not h:
            continue
        connection = _openssl_connect(h, 443)
        for cert_openssl in connection.get_peer_cert_chain():
            cert_der = dump_certificate(FILETYPE_ASN1, cert_openssl)
            cert = Certificate.load(cert_der)
            map_serial_to_name[cert.serial_number] = cert.subject.native

    return map_serial_to_name


if __name__ == "__main__":
    main()
