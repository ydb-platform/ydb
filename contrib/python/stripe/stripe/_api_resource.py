from typing_extensions import Literal, Self

from stripe._error import InvalidRequestError
from stripe._stripe_object import StripeObject
from stripe._request_options import extract_options_from_dict
from stripe._api_mode import ApiMode
from stripe._base_address import BaseAddress
from stripe._api_requestor import _APIRequestor
from stripe import _util
from urllib.parse import quote_plus
from typing import (
    Any,
    ClassVar,
    Generic,
    List,
    Optional,
    TypeVar,
    cast,
    Mapping,
)

T = TypeVar("T", bound=StripeObject)


class APIResource(StripeObject, Generic[T]):
    OBJECT_NAME: ClassVar[str]

    @classmethod
    @_util.deprecated(
        "This method is deprecated and will be removed in a future version of stripe-python. Child classes of APIResource should define their own `retrieve` and use APIResource._request directly."
    )
    def retrieve(cls, id, **params) -> T:
        instance = cls(id, **params)
        instance.refresh()
        return cast(T, instance)

    def refresh(self) -> Self:
        return self._request_and_refresh("get", self.instance_url())

    async def refresh_async(self) -> Self:
        return await self._request_and_refresh_async(
            "get", self.instance_url()
        )

    @classmethod
    def class_url(cls) -> str:
        if cls == APIResource:
            raise NotImplementedError(
                "APIResource is an abstract class.  You should perform "
                "actions on its subclasses (e.g. Charge, Customer)"
            )
        # Namespaces are separated in object names with periods (.) and in URLs
        # with forward slashes (/), so replace the former with the latter.
        base = cls.OBJECT_NAME.replace(".", "/")
        return "/v1/%ss" % (base,)

    def instance_url(self) -> str:
        id = self.get("id")

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

    def _request(
        self,
        method,
        url,
        params=None,
        *,
        base_address: BaseAddress = "api",
        api_mode: ApiMode = "V1",
    ) -> StripeObject:
        obj = StripeObject._request(
            self,
            method,
            url,
            params=params,
            base_address=base_address,
        )

        if type(self) is type(obj):
            self._refresh_from(values=obj, api_mode=api_mode)
            return self
        else:
            return obj

    async def _request_async(
        self,
        method,
        url,
        params=None,
        *,
        base_address: BaseAddress = "api",
        api_mode: ApiMode = "V1",
    ) -> StripeObject:
        obj = await StripeObject._request_async(
            self,
            method,
            url,
            params=params,
            base_address=base_address,
        )

        if type(self) is type(obj):
            self._refresh_from(values=obj, api_mode=api_mode)
            return self
        else:
            return obj

    def _request_and_refresh(
        self,
        method: Literal["get", "post", "delete"],
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        usage: Optional[List[str]] = None,
        *,
        base_address: BaseAddress = "api",
        api_mode: ApiMode = "V1",
    ) -> Self:
        obj = StripeObject._request(
            self,
            method,
            url,
            params=params,
            base_address=base_address,
            usage=usage,
        )

        self._refresh_from(values=obj, api_mode=api_mode)
        return self

    async def _request_and_refresh_async(
        self,
        method: Literal["get", "post", "delete"],
        url: str,
        params: Optional[Mapping[str, Any]] = None,
        usage: Optional[List[str]] = None,
        *,
        base_address: BaseAddress = "api",
        api_mode: ApiMode = "V1",
    ) -> Self:
        obj = await StripeObject._request_async(
            self,
            method,
            url,
            params=params,
            base_address=base_address,
            usage=usage,
        )

        self._refresh_from(values=obj, api_mode=api_mode)
        return self

    @classmethod
    def _static_request(
        cls,
        method_,
        url_,
        params: Optional[Mapping[str, Any]] = None,
        *,
        base_address: BaseAddress = "api",
    ):
        request_options, request_params = extract_options_from_dict(params)
        return _APIRequestor._global_instance().request(
            method_,
            url_,
            params=request_params,
            options=request_options,
            base_address=base_address,
        )

    @classmethod
    async def _static_request_async(
        cls,
        method_,
        url_,
        params: Optional[Mapping[str, Any]] = None,
        *,
        base_address: BaseAddress = "api",
    ):
        request_options, request_params = extract_options_from_dict(params)
        return await _APIRequestor._global_instance().request_async(
            method_,
            url_,
            params=request_params,
            options=request_options,
            base_address=base_address,
        )

    @classmethod
    def _static_request_stream(
        cls,
        method,
        url,
        params: Optional[Mapping[str, Any]] = None,
        *,
        base_address: BaseAddress = "api",
    ):
        request_options, request_params = extract_options_from_dict(params)
        return _APIRequestor._global_instance().request_stream(
            method,
            url,
            params=request_params,
            options=request_options,
            base_address=base_address,
        )

    @classmethod
    async def _static_request_stream_async(
        cls,
        method,
        url,
        params: Optional[Mapping[str, Any]] = None,
        *,
        base_address: BaseAddress = "api",
    ):
        request_options, request_params = extract_options_from_dict(params)
        return await _APIRequestor._global_instance().request_stream_async(
            method,
            url,
            params=request_params,
            options=request_options,
            base_address=base_address,
        )
