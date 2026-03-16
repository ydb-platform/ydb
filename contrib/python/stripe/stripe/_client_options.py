from typing import Optional


class _ClientOptions(object):
    client_id: Optional[str]
    proxy: Optional[str]
    verify_ssl_certs: Optional[bool]

    def __init__(
        self,
        client_id: Optional[str] = None,
        proxy: Optional[str] = None,
        verify_ssl_certs: Optional[bool] = None,
    ):
        self.client_id = client_id
        self.proxy = proxy
        self.verify_ssl_certs = verify_ssl_certs
