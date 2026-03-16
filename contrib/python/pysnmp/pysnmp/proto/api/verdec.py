#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pyasn1.codec.ber import decoder, eoo
from pyasn1.error import PyAsn1Error
from pyasn1.type import univ
from pysnmp.proto.error import ProtocolError


def decode_message_version(wholeMsg):
    """Decode SNMP version from the message."""
    try:
        seq, wholeMsg = decoder.decode(
            wholeMsg,
            asn1Spec=univ.Sequence(),
            recursiveFlag=False,
            substrateFun=lambda a, b, c: (a, b[:c]),
        )
        ver, wholeMsg = decoder.decode(
            wholeMsg,
            asn1Spec=univ.Integer(),
            recursiveFlag=False,
            substrateFun=lambda a, b, c: (a, b[:c]),
        )
        if eoo.endOfOctets.isSameTypeWith(ver):
            raise ProtocolError("EOO at SNMP version component")
        return ver
    except PyAsn1Error as exc:
        raise ProtocolError("Invalid BER at SNMP version component: %s" % exc)
