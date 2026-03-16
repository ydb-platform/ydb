from stripe._api_resource import APIResource

from typing import TypeVar
from stripe._stripe_object import StripeObject

T = TypeVar("T", bound=StripeObject)

# TODO(major): 1704 - Inline into Tax.Settings and Balance, and remove this class.


class SingletonAPIResource(APIResource[T]):
    @classmethod
    def retrieve(cls, **params) -> T:
        return super(SingletonAPIResource, cls).retrieve(None, **params)

    @classmethod
    def class_url(cls):
        if cls == SingletonAPIResource:
            raise NotImplementedError(
                "SingletonAPIResource is an abstract class.  You should "
                "perform actions on its subclasses (e.g. Balance)"
            )
        # Namespaces are separated in object names with periods (.) and in URLs
        # with forward slashes (/), so replace the former with the latter.
        base = cls.OBJECT_NAME.replace(".", "/")
        return "/v1/%s" % (base,)

    def instance_url(self):
        return self.class_url()
