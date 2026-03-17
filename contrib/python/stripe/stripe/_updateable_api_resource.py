from stripe import _util
from stripe._api_resource import APIResource
from urllib.parse import quote_plus
from typing import TypeVar, cast
from stripe._stripe_object import StripeObject

T = TypeVar("T", bound=StripeObject)


class UpdateableAPIResource(APIResource[T]):
    @classmethod
    def modify(cls, sid, **params) -> T:
        url = "%s/%s" % (cls.class_url(), quote_plus(sid))
        return cast(T, cls._static_request("post", url, params=params))

    @_util.deprecated(
        "The `save` method is deprecated and will be removed in a future major version of the library. Use the class method `modify` on the resource instead."
    )
    def save(self, idempotency_key=None):
        updated_params = self.serialize(None)
        if updated_params:
            updated_params["idempotency_key"] = idempotency_key
            self._request_and_refresh(
                "post",
                self.instance_url(),
                params=updated_params,
                usage=["save"],
            )
        else:
            _util.logger.debug("Trying to save already saved object %r", self)
        return self
