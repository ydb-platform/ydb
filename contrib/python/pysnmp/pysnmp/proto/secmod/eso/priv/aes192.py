#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp.proto.secmod.eso.priv import aesbase


class AesBlumenthal192(aesbase.AbstractAesBlumenthal):
    """AES 192 bit encryption (Internet draft).

    Reeder AES encryption:

    http://tools.ietf.org/html/draft-blumenthal-aes-usm-04
    """

    SERVICE_ID = (1, 3, 6, 1, 4, 1, 9, 12, 6, 1, 1)  # cusmAESCfb192PrivProtocol
    KEY_SIZE = 24


class Aes192(aesbase.AbstractAesReeder):
    """AES 192 bit encryption (Internet draft).

    Reeder AES encryption with non-standard key localization algorithm
    borrowed from Reeder 3DES draft:

    http://tools.ietf.org/html/draft-blumenthal-aes-usm-04
    https://tools.ietf.org/html/draft-reeder-snmpv3-usm-3desede-00

    Known to be used by many vendors including Cisco and others.
    """

    SERVICE_ID = (
        1,
        3,
        6,
        1,
        4,
        1,
        9,
        12,
        6,
        1,
        101,
    )  # cusmAESCfb192PrivProtocol (non-standard OID)
    KEY_SIZE = 24
