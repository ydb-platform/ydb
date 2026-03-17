#
# Copyright (c) 2014-2019 Security Innovation, Inc
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0 AND BSD-3-Clause
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/debian-inspector for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import re

"""
Utility to remove PGP signature from messages used to get the RFC822 message
in a .dsc file that uses signature.
Copied from # https://github.com/SecurityInnovation/PGPy/blob/e2893da8b2f5ce0694257caddd8d852aece546ff/pgpy/types.py#L57
and adapted for debian_inspector.
"""


def is_signed(text):
    """
    Return True if the text is likely PGP-signed.
    """
    if text and isinstance(text, str):
        text = text.strip()
        return text and (
            text.startswith("-----BEGIN PGP SIGNED MESSAGE-----")
            and text.endswith("-----END PGP SIGNATURE-----")
        )
    return False


def remove_signature(text):
    """
    Return `text` stripped from a PGP signature if there is one. Return the
    `text` as-is otherwise.
    """
    if not is_signed(text):
        return text

    signed = pgp_signed(text)
    if not signed:
        return text
    unsigned = signed.groupdict().get("cleartext")
    return unsigned


# A re.VERBOSE regular expression to parse a PGP signed message in its parts.
# the re.VERBOSE flag allows for:
#  - whitespace is ignored except when in a character class or escaped
#  - anything after a '#' that is not escaped or in a character class is
#  ignored, allowing for comments


pgp_signed = re.compile(
    r"""
    # This capture group is optional because it will only be present in signed
    # cleartext messages

    (^-{5}BEGIN\ PGP\ SIGNED\ MESSAGE-{5}(?:\r?\n)
       (Hash:\ (?P<hashes>[A-Za-z0-9\-,]+)(?:\r?\n){2})?
       (?P<cleartext>(.*\r?\n)*(.*(?=\r?\n-{5})))(?:\r?\n)
    )?

    # Armor header line: capture the variable part of the magic text

    ^-{5}BEGIN\ PGP\ (?P<magic>[A-Z0-9 ,]+)-{5}(?:\r?\n)

    # Try to capture all the headers into one capture group.
    # If this doesn't match, m['headers'] will be None

    (?P<headers>(^.+:\ .+(?:\r?\n))+)?(?:\r?\n)?

    # capture all lines of the body, up to 76 characters long, including the
    # newline, and the pad character(s)

    (?P<body>([A-Za-z0-9+/]{1,76}={,2}(?:\r?\n))+)

    # capture the armored CRC24 value

    ^=(?P<crc>[A-Za-z0-9+/]{4})(?:\r?\n)

    # finally, capture the armor tail line, which must match the armor header
    # line

    ^-{5}END\ PGP\ (?P=magic)-{5}(?:\r?\n)?

     """,
    flags=re.MULTILINE | re.VERBOSE,
).search  # NOQA
