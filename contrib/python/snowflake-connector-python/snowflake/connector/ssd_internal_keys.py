#!/usr/bin/env python
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from binascii import unhexlify

# key version
ocsp_internal_dep1_key_ver = 0.1
ocsp_internal_dep2_key_ver = 0.1

# OCSP Hard coded public keys
ocsp_internal_ssd_pub_dep1 = None
ocsp_internal_ssd_pub_dep2 = None

# Default cert if for key update directives
SF_KEY_UPDATE_SSD_DEFAULT_CERT_ID = 0


def ret_int_pub_key_ver(issuer):
    if issuer == "dep1":
        return ocsp_internal_dep1_key_ver
    else:
        return ocsp_internal_dep2_key_ver


def ret_wildcard_hkey():
    issuer_name_hash = unhexlify("040130")
    issuer_key_hash = unhexlify("040130")
    serial_number = unhexlify("020100")
    hkey = (issuer_name_hash, issuer_key_hash, serial_number)
    return hkey
