#
# This file is part of pysnmp software.
#
# Copyright (c) 2005-2020, Ilya Etingof <etingof@gmail.com>
# License: https://www.pysnmp.com/pysnmp/license.html
#
from pysnmp.proto import rfc1157, rfc1905

READ_CLASS_PDUS = {
    rfc1157.GetRequestPDU.tagSet: 1,
    rfc1157.GetNextRequestPDU.tagSet: 1,
    rfc1905.GetRequestPDU.tagSet: 1,
    rfc1905.GetNextRequestPDU.tagSet: 1,
    rfc1905.GetBulkRequestPDU.tagSet: 1,
}

WRITE_CLASS_PDUS = {rfc1157.SetRequestPDU.tagSet: 1, rfc1905.SetRequestPDU.tagSet: 1}

RESPONSE_CLASS_PDUS = {
    rfc1157.GetResponsePDU.tagSet: 1,
    rfc1905.ResponsePDU.tagSet: 1,
    rfc1905.ReportPDU.tagSet: 1,
}

NOTIFICATION_CLASS_PDUS = {
    rfc1157.TrapPDU.tagSet: 1,
    rfc1905.SNMPv2TrapPDU.tagSet: 1,
    rfc1905.InformRequestPDU.tagSet: 1,
}

INTERNAL_CLASS_PDUS = {rfc1905.ReportPDU.tagSet: 1}

CONFIRMED_CLASS_PDUS = {
    rfc1157.GetRequestPDU.tagSet: 1,
    rfc1157.GetNextRequestPDU.tagSet: 1,
    rfc1157.SetRequestPDU.tagSet: 1,
    rfc1905.GetRequestPDU.tagSet: 1,
    rfc1905.GetNextRequestPDU.tagSet: 1,
    rfc1905.GetBulkRequestPDU.tagSet: 1,
    rfc1905.SetRequestPDU.tagSet: 1,
    rfc1905.InformRequestPDU.tagSet: 1,
}

UNCONFIRMED_CLASS_PDUS = {
    rfc1157.GetResponsePDU.tagSet: 1,
    rfc1905.ResponsePDU.tagSet: 1,
    rfc1157.TrapPDU.tagSet: 1,
    rfc1905.ReportPDU.tagSet: 1,
    rfc1905.SNMPv2TrapPDU.tagSet: 1,
}
