#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import os
import sys
from os import path

from snowflake.connector.ocsp_asn1crypto import SnowflakeOCSPAsn1Crypto


def main():
    """Internal Tool: Extract certificate files in PEM."""

    def help():
        print(
            "Extract certificate file. The target file can be a single file "
            "or a directory including multiple certificates. The certificate "
            "file format should be PEM."
        )
        print(
            """
Usage: {}  <input file/dir>
""".format(
                path.basename(sys.argv[0])
            )
        )
        sys.exit(2)

    if len(sys.argv) < 2:
        help()

    input_filename = sys.argv[1]
    if path.isdir(input_filename):
        files = [path.join(input_filename, f) for f in os.listdir(input_filename)]
    else:
        files = [input_filename]

    for f in files:
        open(f)
        extract_certificate_file(f)


def extract_certificate_file(input_filename):
    ocsp = SnowflakeOCSPAsn1Crypto()
    cert_map = {}
    ocsp.read_cert_bundle(input_filename, cert_map)

    for cert in cert_map.values():
        print(f"serial #: {cert.serial_number}, name: {cert.subject.native}")


if __name__ == "__main__":
    main()
