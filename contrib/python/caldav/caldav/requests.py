try:
    from niquests.auth import AuthBase
except ImportError:
    from requests.auth import AuthBase


class HTTPBearerAuth(AuthBase):
    def __init__(self, password: str) -> None:
        self.password = password

    def __eq__(self, other: object) -> bool:
        return self.password == getattr(other, "password", None)

    def __ne__(self, other: object) -> bool:
        return not self == other

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.password}"
        return r
