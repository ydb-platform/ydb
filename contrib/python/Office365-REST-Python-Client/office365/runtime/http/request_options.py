from typing import Any

from office365.runtime.http.http_method import HttpMethod


class RequestOptions(object):
    """Request options"""

    def __init__(self, url, method=HttpMethod.Get, data=None):
        """
        Request options

        :param str url: URL for the new :class:`requests.Request` object
        """
        self.url = url
        self.data = data
        self.headers = {}
        self.auth = None
        self.method = method
        self.verify = True
        self.stream = False
        self.proxies = None

    @property
    def is_file(self):
        return hasattr(self.data, "read") and callable(self.data.read)

    @property
    def is_bytes(self):
        return hasattr(self.data, "decode") and callable(self.data.decode)

    def set_header(self, name, value):
        # type: (str, Any) -> None
        self.headers[name] = value

    def ensure_header(self, name, value):
        # type: (str, Any) -> None
        if name not in self.headers:
            self.headers[name] = value
