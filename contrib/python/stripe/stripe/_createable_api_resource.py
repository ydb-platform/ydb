from stripe._api_resource import APIResource
from typing import TypeVar, cast
from stripe._stripe_object import StripeObject

T = TypeVar("T", bound=StripeObject)


class CreateableAPIResource(APIResource[T]):
    @classmethod
    def create(cls, **params) -> T:
        return cast(
            T,
            cls._static_request("post", cls.class_url(), params=params),
        )
