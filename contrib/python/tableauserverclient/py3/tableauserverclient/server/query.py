from collections.abc import Iterable, Iterator, Sized
from itertools import count
from typing import Optional, Protocol, TYPE_CHECKING, TypeVar, overload
import sys
from tableauserverclient.config import config
from tableauserverclient.models.pagination_item import PaginationItem
from tableauserverclient.server.endpoint.exceptions import ServerResponseError
from tableauserverclient.server.filter import Filter
from tableauserverclient.server.request_options import RequestOptions
from tableauserverclient.server.sort import Sort
import math

from typing_extensions import Self

if TYPE_CHECKING:
    from tableauserverclient.server.endpoint import QuerysetEndpoint

T = TypeVar("T")


class Slice(Protocol):
    start: Optional[int]
    step: Optional[int]
    stop: Optional[int]


def to_camel_case(word: str) -> str:
    return word.split("_")[0] + "".join(x.capitalize() or "_" for x in word.split("_")[1:])


"""
This interface allows more fluent queries against Tableau Server
e.g server.users.get(name="user@domain.com")
see pagination_sample
"""


class QuerySet(Iterable[T], Sized):
    """
    QuerySet is a class that allows easy filtering, sorting, and iterating over
    many endpoints in TableauServerClient. It is designed to be used in a similar
    way to Django QuerySets, but with a more limited feature set.

    QuerySet is an iterable, and can be used in for loops, list comprehensions,
    and other places where iterables are expected.

    QuerySet is also Sized, and can be used in places where the length of the
    QuerySet is needed. The length of the QuerySet is the total number of items
    available in the QuerySet, not just the number of items that have been
    fetched. If the endpoint does not return a total count of items, the length
    of the QuerySet will be sys.maxsize. If there is no total count, the
    QuerySet will continue to fetch items until there are no more items to
    fetch.

    QuerySet is not re-entrant. It is not designed to be used in multiple places
    at the same time. If you need to use a QuerySet in multiple places, you
    should create a new QuerySet for each place you need to use it, convert it
    to a list, or create a deep copy of the QuerySet.

    QuerySets are also indexable, and can be sliced. If you try to access an
    index that has not been fetched, the QuerySet will fetch the page that
    contains the item you are looking for.
    """

    def __init__(self, model: "QuerysetEndpoint[T]", page_size: Optional[int] = None) -> None:
        self.model = model
        self.request_options = RequestOptions(pagesize=page_size or config.PAGE_SIZE)
        self._result_cache: list[T] = []
        self._pagination_item = PaginationItem()

    def __iter__(self: Self) -> Iterator[T]:
        # Not built to be re-entrant. Starts back at page 1, and empties
        # the result cache. Ensure the result_cache is empty to not yield
        # items from prior usage.
        self._result_cache = []

        for page in count(1):
            self.request_options.pagenumber = page
            self._result_cache = []
            self._pagination_item._page_number = None
            try:
                self._fetch_all()
            except ServerResponseError as e:
                if e.code == "400006":
                    # If the endpoint does not support pagination, it will end
                    # up overrunning the total number of pages. Catch the
                    # error and break out of the loop.
                    raise StopIteration
            if len(self._result_cache) == 0:
                return
            yield from self._result_cache
            # If the length of the QuerySet is unknown, continue fetching until
            # the result cache is empty.
            if (size := len(self)) == 0:
                continue
            if (page * self.page_size) >= size:
                return

    @overload
    def __getitem__(self: Self, k: Slice) -> list[T]: ...

    @overload
    def __getitem__(self: Self, k: int) -> T: ...

    def __getitem__(self, k):
        page = self.page_number
        size = self.page_size

        # Create a range object for quick checking if k is in the cached result.
        page_range = range((page - 1) * size, page * size)

        if isinstance(k, slice):
            # Parse out the slice object, and assume reasonable defaults if no value provided.
            step = k.step if k.step is not None else 1
            start = k.start if k.start is not None else 0
            stop = k.stop if k.stop is not None else self.total_available

            # If negative values present in slice, convert to positive values
            if start < 0:
                start += self.total_available
            if stop < 0:
                stop += self.total_available
            if start < stop and step < 0:
                # Since slicing is left inclusive and right exclusive, shift
                # the start and stop values by 1 to keep that behavior
                start, stop = stop - 1, start - 1
                slice_stop = stop if stop > 0 else None
                k = slice(start, slice_stop, step)

            # Fetch items from cache if present, otherwise, recursively fetch.
            k_range = range(start, stop, step)
            if all(i in page_range for i in k_range):
                return self._result_cache[k]
            return [self[i] for i in k_range]

        if k < 0:
            k += self.total_available

        if k in page_range:
            # Fetch item from cache if present
            return self._result_cache[k % size]
        elif k in range(self.total_available):
            # Otherwise, check if k is even sensible to return
            self._result_cache = []
            self._pagination_item._page_number = None
            # Add one to k, otherwise it gets stuck at page boundaries, e.g. 100
            self.request_options.pagenumber = max(1, math.ceil((k + 1) / size))
            return self[k]
        else:
            # If k is unreasonable, raise an IndexError.
            raise IndexError

    def _fetch_all(self: Self) -> None:
        """
        Retrieve the data and store result and pagination item in cache
        """
        if not self._result_cache and self._pagination_item._page_number is None:
            response = self.model.get(self.request_options)
            if isinstance(response, tuple):
                self._result_cache, self._pagination_item = response
            else:
                self._result_cache = response
                self._pagination_item = PaginationItem()

    def __len__(self: Self) -> int:
        return sys.maxsize if self.total_available is None else self.total_available

    @property
    def total_available(self: Self) -> int:
        self._fetch_all()
        return self._pagination_item.total_available

    @property
    def page_number(self: Self) -> int:
        self._fetch_all()
        # If the PaginationItem is not returned from the endpoint, use the
        # pagenumber from the RequestOptions.
        return self._pagination_item.page_number or self.request_options.pagenumber

    @property
    def page_size(self: Self) -> int:
        self._fetch_all()
        # If the PaginationItem is not returned from the endpoint, use the
        # pagesize from the RequestOptions.
        return self._pagination_item.page_size or self.request_options.pagesize

    def filter(self: Self, *invalid, page_size: Optional[int] = None, **kwargs) -> Self:
        if invalid:
            raise RuntimeError("Only accepts keyword arguments.")
        for kwarg_key, value in kwargs.items():
            field_name, operator = self._parse_shorthand_filter(kwarg_key)
            self.request_options.filter.add(Filter(field_name, operator, value))

        if page_size:
            self.request_options.pagesize = page_size
        return self

    def order_by(self: Self, *args) -> Self:
        for arg in args:
            field_name, direction = self._parse_shorthand_sort(arg)
            self.request_options.sort.add(Sort(field_name, direction))
        return self

    def paginate(self: Self, **kwargs) -> Self:
        if "page_number" in kwargs:
            self.request_options.pagenumber = kwargs["page_number"]
        if "page_size" in kwargs:
            self.request_options.pagesize = kwargs["page_size"]
        return self

    def fields(self: Self, *fields: str) -> Self:
        """
        Add fields to the request options. If no fields are provided, the
        default fields will be used. If fields are provided, the default fields
        will be used in addition to the provided fields.

        Parameters
        ----------
        fields : str
            The fields to include in the request options.

        Returns
        -------
        QuerySet
        """
        self.request_options.fields |= set(fields) | set(("_default_"))
        return self

    def only_fields(self: Self, *fields: str) -> Self:
        """
        Add fields to the request options. If no fields are provided, the
        default fields will be used. If fields are provided, the default fields
        will be replaced by the provided fields.

        Parameters
        ----------
        fields : str
            The fields to include in the request options.

        Returns
        -------
        QuerySet
        """
        self.request_options.fields |= set(fields)
        return self

    @staticmethod
    def _parse_shorthand_filter(key: str) -> tuple[str, str]:
        tokens = key.split("__", 1)
        if len(tokens) == 1:
            operator = RequestOptions.Operator.Equals
        else:
            operator = tokens[1]
            if operator not in RequestOptions.Operator.__dict__.values():
                raise ValueError(f"Operator `{operator}` is not valid.")

        field = to_camel_case(tokens[0])
        if field not in RequestOptions.Field.__dict__.values():
            raise ValueError(f"Field name `{field}` is not valid.")
        return (field, operator)

    @staticmethod
    def _parse_shorthand_sort(key: str) -> tuple[str, str]:
        direction = RequestOptions.Direction.Asc
        if key.startswith("-"):
            direction = RequestOptions.Direction.Desc
            key = key[1:]

        key = to_camel_case(key)
        if key not in RequestOptions.Field.__dict__.values():
            raise ValueError("Sort key name %s is not valid.", key)
        return (key, direction)
