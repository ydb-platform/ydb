from stripe import _util
from stripe._api_resource import APIResource
from urllib.parse import quote_plus
from typing import TypeVar, cast
from stripe._stripe_object import StripeObject

T = TypeVar("T", bound=StripeObject)


class DeletableAPIResource(APIResource[T]):
    @classmethod
    def _cls_delete(cls, sid, **params) -> T:
        url = "%s/%s" % (cls.class_url(), quote_plus(sid))
        return cast(T, cls._static_request("delete", url, params=params))

    @_util.class_method_variant("_cls_delete")
    def delete(self, **params) -> T:
        return cast(
            T,
            self._request_and_refresh(
                "delete", self.instance_url(), params=params
            ),
        )
