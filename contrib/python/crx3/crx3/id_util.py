# -*- coding: utf-8 -*-

import hashlib


def calc_crx_id_by_public_key(public_key):
    """
    Calculate hexadecimal crx id by public key data.
    :param public_key: public key bytes
    :return: hexadecimal crx id bytes
    """
    sha256 = hashlib.sha256()
    sha256.update(public_key)
    digest = sha256.digest()
    return digest[0:16]


def convert_hex_crx_id_to_alphabet(hex_crx_id):
    """
    Convert the hexadecimal crx id to alphabet format.
    Refer: https://source.chromium.org/chromium/chromium/src/+/main:components/crx_file/id_util.cc
    :param hex_crx_id: hexadecimal crx id
    :return: alphabet crx id string
    """
    alphabet_chars = []
    for b in hex_crx_id:
        alphabet_chars.append(chr(int(b, 16) + 97))
    return ''.join(alphabet_chars)
