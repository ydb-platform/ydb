"""File-backed monitoring credentials; never serialize secrets into reports."""
import ipaddress
import json
import os
import ssl
import stat
import urllib.error
import urllib.parse
import urllib.request


def _origin(url):
    parsed = urllib.parse.urlsplit(url)
    return parsed.scheme, parsed.hostname, parsed.port or (443 if parsed.scheme == "https" else 80)


class _SameOriginRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        if _origin(req.full_url) != _origin(newurl):
            raise urllib.error.URLError("monitoring credentials cannot follow a cross-origin redirect")
        return super().redirect_request(req, fp, code, msg, headers, newurl)


class HttpCredentials:
    def __init__(self, headers):
        self._headers = headers

    def __repr__(self):
        return "HttpCredentials(<redacted>)"

    def validate_endpoint(self, endpoint, insecure=False):
        parsed = urllib.parse.urlsplit(endpoint)
        if parsed.username is not None or parsed.password is not None or parsed.query or parsed.fragment:
            raise ValueError("monitoring endpoint must not contain credentials, query or fragment")
        loopback = parsed.hostname == "localhost"
        try:
            loopback = loopback or ipaddress.ip_address(parsed.hostname or "").is_loopback
        except ValueError:
            pass
        if insecure or (parsed.scheme != "https" and not (parsed.scheme == "http" and loopback)):
            raise ValueError("monitoring credentials require verified HTTPS or a loopback SSH tunnel; do not use --insecure")

    def open(self, url, timeout):
        self.validate_endpoint(url.split("?", 1)[0])
        request = urllib.request.Request(url, headers=self._headers)
        handlers = [
            _SameOriginRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context())
        ]
        if urllib.parse.urlsplit(url).scheme == "http":
            # HTTP credentials are permitted only on loopback. Do not let an
            # environment proxy send that supposedly local request elsewhere.
            handlers.append(urllib.request.ProxyHandler({}))
        opener = urllib.request.build_opener(*handlers)
        return opener.open(request, timeout=timeout)


def load_credentials(path, endpoint, insecure=False):
    if not path:
        return None
    if not endpoint:
        raise ValueError("--mon-credentials-file requires --mon-endpoint")
    with open(path, encoding="utf-8") as stream:
        if os.fstat(stream.fileno()).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise ValueError("credentials file must be private: chmod 600 <credentials-file>")
        try:
            headers = json.load(stream)
        except ValueError:
            raise ValueError("credentials file must contain a JSON object of HTTP headers") from None
    if not isinstance(headers, dict) or not headers:
        raise ValueError("credentials file must contain a nonempty JSON object")
    for name, value in headers.items():
        if name.lower() not in ("authorization", "cookie"):
            raise ValueError("credentials file supports only Authorization and Cookie headers")
        if not isinstance(value, str) or not value or any(ord(c) < 32 or ord(c) > 126 for c in value):
            raise ValueError("credential header values must be nonempty single-line ASCII strings")
    credentials = HttpCredentials(headers)
    credentials.validate_endpoint(endpoint, insecure)
    return credentials
