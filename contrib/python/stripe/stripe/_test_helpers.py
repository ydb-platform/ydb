from stripe._error import InvalidRequestError
from urllib.parse import quote_plus

from typing import TypeVar, ClassVar, Any
from typing_extensions import Protocol
from stripe._api_resource import APIResource

T = TypeVar("T", bound=APIResource[Any])


class APIResourceTestHelpers(Protocol[T]):
    """
    The base type for the TestHelper nested classes.
    Handles request URL generation for test_helper custom methods.
    Should be used in combination with the @test_helpers decorator.

    @test_helpers
    class Foo(APIResource):
      class TestHelpers(APIResourceTestHelpers):
    """

    _resource_cls: ClassVar[Any]
    resource: T

    def __init__(self, resource):
        self.resource = resource

    @classmethod
    def _static_request(cls, *args, **kwargs):
        return cls._resource_cls._static_request(*args, **kwargs)

    @classmethod
    async def _static_request_async(cls, *args, **kwargs):
        return await cls._resource_cls._static_request_async(*args, **kwargs)

    @classmethod
    def _static_request_stream(cls, *args, **kwargs):
        return cls._resource_cls._static_request_stream(*args, **kwargs)

    @classmethod
    def class_url(cls):
        if cls == APIResourceTestHelpers:
            raise NotImplementedError(
                "APIResourceTestHelpers is an abstract class.  You should perform "
                "actions on its subclasses (e.g. Charge, Customer)"
            )
        # Namespaces are separated in object names with periods (.) and in URLs
        # with forward slashes (/), so replace the former with the latter.
        base = cls._resource_cls.OBJECT_NAME.replace(".", "/")
        return "/v1/test_helpers/%ss" % (base,)

    def instance_url(self):
        id = getattr(self.resource, "id", None)

        if not isinstance(id, str):
            raise InvalidRequestError(
                "Could not determine which URL to request: %s instance "
                "has invalid ID: %r, %s. ID should be of type `str` (or"
                " `unicode`)" % (type(self).__name__, id, type(id)),
                "id",
            )

        base = self.class_url()
        extn = quote_plus(id)
        return "%s/%s" % (base, extn)
