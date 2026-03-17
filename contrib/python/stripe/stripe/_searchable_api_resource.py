from stripe._api_resource import APIResource
from stripe._search_result_object import SearchResultObject
from typing import TypeVar
from typing_extensions import TYPE_CHECKING

if TYPE_CHECKING:
    from stripe._stripe_object import StripeObject

T = TypeVar("T", bound="StripeObject")


class SearchableAPIResource(APIResource[T]):
    @classmethod
    def _search(cls, search_url, **params):
        ret = cls._static_request(
            "get",
            search_url,
            params=params,
        )
        if not isinstance(ret, SearchResultObject):
            raise TypeError(
                "Expected search result from API, got %s"
                % (type(ret).__name__,)
            )

        return ret

    @classmethod
    async def _search_async(cls, search_url, **params):
        ret = await cls._static_request_async(
            "get",
            search_url,
            params=params,
        )
        if not isinstance(ret, SearchResultObject):
            raise TypeError(
                "Expected search result from API, got %s"
                % (type(ret).__name__,)
            )

        return ret

    @classmethod
    def search(cls, *args, **kwargs):
        raise NotImplementedError

    @classmethod
    def search_auto_paging_iter(cls, *args, **kwargs):
        raise NotImplementedError
