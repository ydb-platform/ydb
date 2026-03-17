from typing import Any, Dict
from typing_extensions import Protocol

from stripe._stripe_object import StripeObject


class _Verifiable(Protocol):
    def instance_url(self) -> str: ...

    def _request(
        self,
        method: str,
        url: str,
        params: Dict[str, Any],
    ) -> StripeObject: ...


class VerifyMixin(object):
    def verify(self: _Verifiable, **params):
        url = self.instance_url() + "/verify"
        return self._request("post", url, params=params)
