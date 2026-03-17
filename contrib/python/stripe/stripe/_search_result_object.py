# pyright: strict
from typing_extensions import Self, Unpack
from typing import (
    Generic,
    List,
    TypeVar,
    cast,
    Any,
    Mapping,
    Iterator,
    AsyncIterator,
    Optional,
)

from stripe._api_requestor import (
    _APIRequestor,  # pyright: ignore[reportPrivateUsage]
)
from stripe._stripe_object import StripeObject
from stripe import _util
import warnings
from stripe._request_options import RequestOptions, extract_options_from_dict
from stripe._any_iterator import AnyIterator

T = TypeVar("T", bound=StripeObject)


class SearchResultObject(StripeObject, Generic[T]):
    OBJECT_NAME = "search_result"
    data: List[T]
    has_more: bool
    next_page: str

    def _search(self, **params: Mapping[str, Any]) -> Self:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            return self.search(  # pyright: ignore[reportDeprecated]
                **params,
            )

    def _get_url_for_search(self) -> str:
        url = self.get("url")
        if not isinstance(url, str):
            raise ValueError(
                'Cannot call .list on a list object without a string "url" property'
            )
        return url

    @_util.deprecated(
        "This will be removed in a future version of stripe-python. Please call the `search` method on the corresponding resource directly, instead of the generic search on SearchResultObject."
    )
    def search(self, **params: Mapping[str, Any]) -> Self:
        return cast(
            Self,
            self._request(
                "get",
                self._get_url_for_search(),
                params=params,
                base_address="api",
            ),
        )

    async def _search_async(self, **params: Mapping[str, Any]) -> Self:
        return cast(
            Self,
            await self._request_async(
                "get",
                self._get_url_for_search(),
                params=params,
                base_address="api",
            ),
        )

    def __getitem__(self, k: str) -> T:
        if isinstance(k, str):  # pyright: ignore
            return super(SearchResultObject, self).__getitem__(k)
        else:
            raise KeyError(
                "You tried to access the %s index, but SearchResultObject types "
                "only support string keys. (HINT: Search calls return an object "
                "with  a 'data' (which is the data array). You likely want to "
                "call .data[%s])" % (repr(k), repr(k))
            )

    #  Pyright doesn't like this because SearchResultObject inherits from StripeObject inherits from Dict[str, Any]
    #  and so it wants the type of __iter__ to agree with __iter__ from Dict[str, Any]
    #  But we are iterating through "data", which is a List[T].
    def __iter__(self) -> Iterator[T]:  # pyright: ignore
        return getattr(self, "data", []).__iter__()

    def __len__(self) -> int:
        return getattr(self, "data", []).__len__()

    def _auto_paging_iter(self) -> Iterator[T]:
        page = self

        while True:
            for item in page:
                yield item
            page = page.next_search_result_page()

            if page.is_empty:
                break

    def auto_paging_iter(self) -> AnyIterator[T]:
        return AnyIterator(
            self._auto_paging_iter(), self._auto_paging_iter_async()
        )

    async def _auto_paging_iter_async(self) -> AsyncIterator[T]:
        page = self

        while True:
            for item in page:
                yield item
            page = await page.next_search_result_page_async()

            if page.is_empty:
                break

    @classmethod
    def _empty_search_result(
        cls,
        **params: Unpack[RequestOptions],
    ) -> Self:
        return cls._construct_from(
            values={"data": [], "has_more": False, "next_page": None},
            last_response=None,
            requestor=_APIRequestor._global_with_options(  # pyright: ignore[reportPrivateUsage]
                **params,
            ),
            api_mode="V1",
        )

    @property
    def is_empty(self) -> bool:
        return not self.data

    def _get_filters_for_next_page(
        self, params: RequestOptions
    ) -> Mapping[str, Any]:
        params_with_filters = dict(self._retrieve_params)
        params_with_filters.update({"page": self.next_page})
        params_with_filters.update(params)
        return params_with_filters

    def _maybe_empty_result(self, params: RequestOptions) -> Optional[Self]:
        if not self.has_more:
            options, _ = extract_options_from_dict(params)
            return self._empty_search_result(
                api_key=options.get("api_key"),
                stripe_version=options.get("stripe_version"),
                stripe_account=options.get("stripe_account"),
            )
        return None

    def next_search_result_page(
        self, **params: Unpack[RequestOptions]
    ) -> Self:
        empty = self._maybe_empty_result(params)
        return (
            empty
            if empty is not None
            else self._search(
                **self._get_filters_for_next_page(params),
            )
        )

    async def next_search_result_page_async(
        self, **params: Unpack[RequestOptions]
    ) -> Self:
        empty = self._maybe_empty_result(params)
        return (
            empty
            if empty is not None
            else await self._search_async(
                **self._get_filters_for_next_page(params),
            )
        )
