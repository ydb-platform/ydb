# $Id: sip.py 48 2008-05-27 17:31:15Z yardley $
# -*- coding: utf-8 -*-
"""Session Initiation Protocol."""
from __future__ import absolute_import

from . import http


class Request(http.Request):
    """SIP request.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of SIP request.
        TODO.
    """

    __hdr_defaults__ = {
        'method': 'INVITE',
        'uri': 'sip:user@example.com',
        'version': '2.0',
        'headers': {'To': '', 'From': '', 'Call-ID': '', 'CSeq': '', 'Contact': ''}
    }
    __methods = dict.fromkeys((
        'ACK', 'BYE', 'CANCEL', 'INFO', 'INVITE', 'MESSAGE', 'NOTIFY',
        'OPTIONS', 'PRACK', 'PUBLISH', 'REFER', 'REGISTER', 'SUBSCRIBE',
        'UPDATE'
    ))
    __proto = 'SIP'


class Response(http.Response):
    """SIP response.

    TODO: Longer class information....

    Attributes:
        __hdr__: Header fields of SIP response.
        TODO.
    """

    __hdr_defaults__ = {
        'version': '2.0',
        'status': '200',
        'reason': 'OK',
        'headers': {'To': '', 'From': '', 'Call-ID': '', 'CSeq': '', 'Contact': ''}
    }
    __proto = 'SIP'
