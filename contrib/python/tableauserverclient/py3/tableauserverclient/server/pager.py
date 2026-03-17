import copy
from functools import partial
from typing import Optional, Protocol, TypeVar, Union, runtime_checkable
from collections.abc import Iterable, Iterator

from tableauserverclient.models.pagination_item import PaginationItem
from tableauserverclient.server.request_options import RequestOptions


T = TypeVar("T")


@runtime_checkable
class Endpoint(Protocol[T]):
    def get(self, req_options: Optional[RequestOptions]) -> tuple[list[T], PaginationItem]: ...


@runtime_checkable
class CallableEndpoint(Protocol[T]):
    def __call__(self, __req_options: Optional[RequestOptions], **kwargs) -> tuple[list[T], PaginationItem]: ...


class Pager(Iterable[T]):
    """
    Generator that takes an endpoint (top level endpoints with `.get)` and lazily loads items from Server.
    Supports all `RequestOptions` including starting on any page. Also used by models to load sub-models
    (users in a group, views in a workbook, etc) by passing a different endpoint.

    Will loop over anything that returns (list[ModelItem], PaginationItem).

    Will make a copy of the `RequestOptions` object passed in so it can be reused.

    Makes a call to the Server for each page of items, then yields each item in the list.

    Parameters
    ----------
    endpoint: CallableEndpoint[T] or Endpoint[T]
        The endpoint to call to get the items. Can be a callable or an Endpoint object.
        Expects a tuple of (list[T], PaginationItem) to be returned.

    request_opts: RequestOptions, optional
        The request options to pass to the endpoint. If not provided, will use default RequestOptions.
        Filters, sorts, page size, starting page number, etc can be set here.

    Yields
    ------
    T
        The items returned from the endpoint.

    Raises
    ------
    ValueError
        If the endpoint is not a callable or an Endpoint object.
    """

    def __init__(
        self,
        endpoint: Union[CallableEndpoint[T], Endpoint[T]],
        request_opts: Optional[RequestOptions] = None,
        **kwargs,
    ) -> None:
        if isinstance(endpoint, Endpoint):
            # The simpliest case is to take an Endpoint and call its get
            endpoint = partial(endpoint.get, **kwargs)
            self._endpoint = endpoint
        elif isinstance(endpoint, CallableEndpoint):
            # but if they pass a callable then use that instead (used internally)
            endpoint = partial(endpoint, **kwargs)
            self._endpoint = endpoint
        else:
            # Didn't get something we can page over
            raise ValueError("Pager needs a server endpoint to page through.")

        self._options = request_opts or RequestOptions()

    def __iter__(self) -> Iterator[T]:
        options = copy.deepcopy(self._options)
        while True:
            # Fetch the first page
            current_item_list, pagination_item = self._endpoint(options)

            if pagination_item.total_available is None:
                # This endpoint does not support pagination, drain the list and return
                yield from current_item_list
                return
            yield from current_item_list

            if pagination_item.page_size * pagination_item.page_number >= pagination_item.total_available:
                # Last page, exit
                return

            # Update the options to fetch the next page
            options.pagenumber = pagination_item.page_number + 1
            options.pagesize = pagination_item.page_size
