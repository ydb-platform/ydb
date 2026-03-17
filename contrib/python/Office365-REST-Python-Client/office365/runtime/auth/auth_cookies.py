import json
from typing import Dict, Optional


class AuthCookies:
    def __init__(self, cookies):
        # type: (Dict) -> None
        self._cookies = cookies

    @property
    def is_valid(self) -> bool:
        """Validates authorization cookies."""
        return bool(self._cookies) and (
            self.fed_auth is not None or self.spo_idcrl is not None
        )

    @property
    def cookie_header(self):
        """Converts stored cookies into an HTTP Cookie header string."""
        return "; ".join(f"{key}={val}" for key, val in self._cookies.items())

    @property
    def fed_auth(self):
        # type: () -> Optional[str]
        """Returns the Primary authentication token."""
        return self._cookies.get("FedAuth", None)

    @property
    def spo_idcrl(self):
        # type: () -> Optional[str]
        """Returns the secondary authentication token. (SharePoint Online Identity CRL)"""
        return self._cookies.get("SPOIDCRL", None)

    @property
    def rt_fa(self):
        # type: () -> Optional[str]
        """Returns the refresh token for Federated Authentication."""
        return self._cookies.get("rtFa", None)

    def to_json(self):
        """Serializes cookies to JSON format."""
        return json.dumps(self._cookies, indent=2)

    @classmethod
    def from_json(cls, json_data):
        """Deserializes cookies from JSON format."""
        cookies_dict = json.loads(json_data)
        return cls(cookies_dict)
