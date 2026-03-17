###############################################################################
# Copyright (c) 2018, Lawrence Livermore National Security, LLC
# Produced at the Lawrence Livermore National Laboratory
# Written by Thomas Mendoza mendoza33@llnl.gov
# LLNL-CODE-754897
# All rights reserved
#
# This file is part of Certipy: https://github.com/LLNL/certipy
#
# SPDX-License-Identifier: BSD-3-Clause
###############################################################################

import argparse
import sys

from certipy import (
    Certipy,
    CertExistsError,
    CertificateAuthorityInUseError,
    KeyType,
)


def main():
    describe_certipy = """
        Certipy: Create simple, self-signed certificate authorities and certs.
    """
    parser = argparse.ArgumentParser(description=describe_certipy)
    parser.add_argument(
        "name",
        help="""Name of the cert to create,
                        defaults to creating a CA cert. If no signing
                        --ca-name specified.""",
    )
    parser.add_argument(
        "--ca-name", help="The name of the CA to sign this cert.", default=""
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="If the cert already exists, bump the serial and overwrite it.",
    )
    parser.add_argument(
        "--rm", action="store_true", help="Remove the cert specified by name."
    )
    parser.add_argument(
        "--cert-type",
        default="rsa",
        choices=[t.value for t in KeyType],
        help="The type of key to create.",
    )
    parser.add_argument(
        "--bits", type=int, default=2048, help="The number of bits to use."
    )
    parser.add_argument(
        "--valid", type=int, default=5, help="Years the cert is valid for."
    )
    parser.add_argument(
        "--alt-names",
        default="",
        help="Alt names for the certificate (comma delimited).",
    )
    parser.add_argument(
        "--store-dir", default="out", help="The location for the store and certs."
    )

    args = parser.parse_args()

    certipy = Certipy(store_dir=args.store_dir)
    record = None

    if args.rm:
        try:
            record = certipy.store.remove_files(args.name, delete_dir=True)
            print("Deleted:")
            for key, val in record.items():
                print(key.upper(), val)
        except CertificateAuthorityInUseError as e:
            print("Unable to delete.", e)
        sys.exit(0)

    alt_names = None
    if args.alt_names:
        alt_names = [_.strip() for _ in args.alt_names.split(",")]

    if args.ca_name:
        ca_record = certipy.store.get_record(args.ca_name)
        if ca_record:
            try:
                record = certipy.create_signed_pair(
                    args.name,
                    args.ca_name,
                    cert_type=args.cert_type,
                    bits=args.bits,
                    years=args.valid,
                    alt_names=alt_names,
                    overwrite=args.overwrite,
                )
            except CertExistsError as e:
                print(e)
        else:
            print(
                "CA {} not found. Must specify an exisiting authority to"
                " sign this cert.".format(args.ca_name)
            )
    else:
        try:
            record = certipy.create_ca(
                args.name,
                cert_type=args.cert_type,
                bits=args.bits,
                years=args.valid,
                alt_names=alt_names,
                overwrite=args.overwrite,
            )
        except CertExistsError as e:
            print(e)

    if record:
        for key, val in record.items():
            print(key.upper(), val)
