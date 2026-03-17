# Copyright (c) 2015, Menno Smits
# Released subject to the New BSD License
# Please see http://en.wikipedia.org/wiki/BSD_licenses

import argparse
import configparser
import json
import os
import ssl
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, Optional, Tuple, TYPE_CHECKING, TypeVar

import imapclient


def getenv(name: str, default: Optional[str]) -> Optional[str]:
    return os.environ.get("imapclient_" + name, default)


def get_config_defaults() -> Dict[str, Any]:
    return {
        "username": getenv("username", None),
        "password": getenv("password", None),
        "ssl": True,
        "ssl_check_hostname": True,
        "ssl_verify_cert": True,
        "ssl_ca_file": None,
        "timeout": None,
        "starttls": False,
        "stream": False,
        "oauth2": False,
        "oauth2_client_id": getenv("oauth2_client_id", None),
        "oauth2_client_secret": getenv("oauth2_client_secret", None),
        "oauth2_refresh_token": getenv("oauth2_refresh_token", None),
        "expect_failure": None,
    }


def parse_config_file(filename: str) -> argparse.Namespace:
    """Parse INI files containing IMAP connection details.

    Used by livetest.py and interact.py
    """

    parser = configparser.ConfigParser(get_string_config_defaults())
    parser.read(filename)

    conf = _read_config_section(parser, "DEFAULT")
    if conf.expect_failure:
        raise ValueError("expect_failure should not be set for the DEFAULT section")

    conf.alternates = {}
    for section in parser.sections():
        # pylint: disable=no-member
        conf.alternates[section] = _read_config_section(parser, section)

    return conf


def get_string_config_defaults() -> Dict[str, str]:
    out = {}
    for k, v in get_config_defaults().items():
        if v is True:
            v = "true"
        elif v is False:
            v = "false"
        elif not v:
            v = ""
        out[k] = v
    return out


T = TypeVar("T")


def _read_config_section(
    parser: configparser.ConfigParser, section: str
) -> argparse.Namespace:
    def get(name: str) -> str:
        return parser.get(section, name)

    def getboolean(name: str) -> bool:
        return parser.getboolean(section, name)

    def get_allowing_none(name: str, typefunc: Callable[[str], T]) -> Optional[T]:
        try:
            v = parser.get(section, name)
        except configparser.NoOptionError:
            return None
        if not v:
            return None
        return typefunc(v)

    def getint(name: str) -> Optional[int]:
        return get_allowing_none(name, int)

    def getfloat(name: str) -> Optional[float]:
        return get_allowing_none(name, float)

    ssl_ca_file = get("ssl_ca_file")
    if ssl_ca_file:
        ssl_ca_file = os.path.expanduser(ssl_ca_file)

    return argparse.Namespace(
        host=get("host"),
        port=getint("port"),
        ssl=getboolean("ssl"),
        starttls=getboolean("starttls"),
        ssl_check_hostname=getboolean("ssl_check_hostname"),
        ssl_verify_cert=getboolean("ssl_verify_cert"),
        ssl_ca_file=ssl_ca_file,
        timeout=getfloat("timeout"),
        stream=getboolean("stream"),
        username=get("username"),
        password=get("password"),
        oauth2=getboolean("oauth2"),
        oauth2_client_id=get("oauth2_client_id"),
        oauth2_client_secret=get("oauth2_client_secret"),
        oauth2_refresh_token=get("oauth2_refresh_token"),
        expect_failure=get("expect_failure"),
    )


OAUTH2_REFRESH_URLS = {
    "imap.gmail.com": "https://accounts.google.com/o/oauth2/token",
    "imap.mail.yahoo.com": "https://api.login.yahoo.com/oauth2/get_token",
}


def refresh_oauth2_token(
    hostname: str, client_id: str, client_secret: str, refresh_token: str
) -> str:
    url = OAUTH2_REFRESH_URLS.get(hostname)
    if not url:
        raise ValueError("don't know where to refresh OAUTH2 token for %r" % hostname)

    post = {
        "client_id": client_id.encode("ascii"),
        "client_secret": client_secret.encode("ascii"),
        "refresh_token": refresh_token.encode("ascii"),
        "grant_type": b"refresh_token",
    }
    with urllib.request.urlopen(
        url, urllib.parse.urlencode(post).encode("ascii")
    ) as request:
        response = request.read()
    result = json.loads(response.decode("ascii"))["access_token"]
    if TYPE_CHECKING:
        assert isinstance(result, str)
    return result


# Tokens are expensive to refresh so use the same one for the duration of the process.
_oauth2_cache: Dict[Tuple[str, str, str, str], str] = {}


def get_oauth2_token(
    hostname: str, client_id: str, client_secret: str, refresh_token: str
) -> str:
    cache_key = (hostname, client_id, client_secret, refresh_token)
    token = _oauth2_cache.get(cache_key)
    if token:
        return token

    token = refresh_oauth2_token(hostname, client_id, client_secret, refresh_token)
    _oauth2_cache[cache_key] = token
    return token


def create_client_from_config(
    conf: argparse.Namespace, login: bool = True
) -> imapclient.IMAPClient:
    assert conf.host, "missing host"

    ssl_context = None
    if conf.ssl:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = conf.ssl_check_hostname
        if not conf.ssl_verify_cert:
            ssl_context.verify_mode = ssl.CERT_NONE
        if conf.ssl_ca_file:
            ssl_context.load_verify_locations(cafile=conf.ssl_ca_file)

    client = imapclient.IMAPClient(
        conf.host,
        port=conf.port,
        ssl=conf.ssl,
        ssl_context=ssl_context,
        stream=conf.stream,
        timeout=conf.timeout,
    )
    if not login:
        return client

    try:
        if conf.starttls:
            client.starttls()

        if conf.oauth2:
            assert conf.oauth2_client_id, "missing oauth2 id"
            assert conf.oauth2_client_secret, "missing oauth2 secret"
            assert conf.oauth2_refresh_token, "missing oauth2 refresh token"
            access_token = get_oauth2_token(
                conf.host,
                conf.oauth2_client_id,
                conf.oauth2_client_secret,
                conf.oauth2_refresh_token,
            )
            client.oauth2_login(conf.username, access_token)

        elif not conf.stream:
            assert conf.username, "missing username"
            assert conf.password, "missing password"
            client.login(conf.username, conf.password)
        return client
    except:  # noqa: E722
        client.shutdown()
        raise
