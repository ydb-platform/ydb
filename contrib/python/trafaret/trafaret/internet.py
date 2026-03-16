# -*- coding: utf-8 -*-

import re
from .regexp import Regexp, RegexpString
from .base import String, FromBytes, OnError, WithRepr
from .lib import py3
from . import codes

if py3:
    import urllib.parse as urlparse
else:  # pragma: no cover
    import urlparse


MAX_EMAIL_LEN = 254


EMAIL_REGEXP = re.compile(
    # dot-atom
    r"(?P<name>^[-!#$%&'*+/=?^_`{}|~0-9A-Z]+(\.[-!#$%&'*+/=?^_`{}|~0-9A-Z]+)*"
    # quoted-string
    r'|^"([\001-\010\013\014\016-\037!#-\[\]-\177]|\\[\001-011\013\014\016-\177])*"'
    # domain
    r')@(?P<domain>(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)$)'
    # literal form, ipv4 address (SMTP 4.1.3)
    r'|\[(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}\]$',
    re.IGNORECASE,
)


def email_idna_encode(value):
    if '@' in value:
        parts = value.split('@')
        parts[-1] = parts[-1].encode('idna').decode('ascii')
        return '@'.join(parts)
    return value


to_str = OnError(FromBytes('utf-8') | String(), 'value is not a string', code=codes.IS_NOT_A_STRING)


email_regexp_trafaret = OnError(to_str & Regexp(EMAIL_REGEXP), 'value is not a valid email address')
email_trafaret = (email_regexp_trafaret | (to_str & email_idna_encode & email_regexp_trafaret))
Email = to_str & OnError(
    String(max_length=MAX_EMAIL_LEN) & email_trafaret,
    'value is not a valid email address',
    code=codes.IS_NOT_VALID_EMAIL,
)
Email = WithRepr(Email, '<Email>')


class Hex(RegexpString):
    regex = r'^[0-9a-f]*$'
    str_method = "lower"

    def __repr__(self):
        return '<Hex>'


class URLSafe(RegexpString):
    regex = r'^[0-9A-Za-z-_]*$'

    def __repr__(self):
        return '<URLSafe>'


URL_REGEXP = re.compile(
    r'^(?:http|ftp)s?://'  # http:// or https://
    r'(?:\S+(?::\S*)?@)?'  # user and password
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-_]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'  # domain...
    r'localhost|'  # localhost...
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
    r'(?::\d+)?'  # optional port
    r'(?:/?|[/?]\S+)$',
    re.IGNORECASE,
)
URLRegexp = to_str & Regexp(URL_REGEXP)


def decode_url_idna(value):
    scheme, netloc, path, query, fragment = urlparse.urlsplit(value)
    netloc = netloc.encode('idna').decode('ascii')  # IDN -> ACE
    return urlparse.urlunsplit((scheme, netloc, path, query, fragment))


URL = OnError(
    URLRegexp | (to_str & decode_url_idna & URLRegexp),
    'value is not URL',
    code=codes.IS_NOT_VALID_URL,
)
URL = WithRepr(URL, '<URL>')


IPv4 = OnError(
    Regexp(
        r'^((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])$',  # noqa
    ),
    'value is not IPv4 address',
    code=codes.IS_NOT_IPv4,
)
IPv4 = WithRepr(IPv4, '<IPv4>')


IPv6 = OnError(
    Regexp(
        r'^('
        r'(::)|'
        r'(::[0-9a-f]{1,4})|'
        r'([0-9a-f]{1,4}:){7,7}[0-9a-f]{1,4}|'
        r'([0-9a-f]{1,4}:){1,7}:|'
        r'([0-9a-f]{1,4}:){1,6}:[0-9a-f]{1,4}|'
        r'([0-9a-f]{1,4}:){1,5}(:[0-9a-f]{1,4}){1,2}|'
        r'([0-9a-f]{1,4}:){1,4}(:[0-9a-f]{1,4}){1,3}|'
        r'([0-9a-f]{1,4}:){1,3}(:[0-9a-f]{1,4}){1,4}|'
        r'([0-9a-f]{1,4}:){1,2}(:[0-9a-f]{1,4}){1,5}|'
        r'[0-9a-f]{1,4}:((:[0-9a-f]{1,4}){1,6})|'
        r':((:[0-9a-f]{1,4}){1,7}:)|'
        r'fe80:(:[0-9a-f]{0,4}){0,4}%[0-9a-z]{1,}|'
        r'::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|'  # noqa
        r'([0-9a-f]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])'  # noqa
        r')$',
        re.IGNORECASE,
    ),
    'value is not IPv6 address',
    code=codes.IS_NOT_IPv6,
)
IPv6 = WithRepr(IPv6, '<IPv6>')


IP = OnError(IPv4 | IPv6, 'value is not IP address', code=codes.IS_NOT_IP)
IP = WithRepr(IP, '<IP>')
