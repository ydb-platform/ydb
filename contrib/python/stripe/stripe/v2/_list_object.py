from stripe._stripe_object import StripeObject
from typing import List, Optional, TypeVar, Generic


T = TypeVar("T", bound=StripeObject)


class ListObject(StripeObject, Generic[T]):
    """
    Represents one page of a list of V2 Stripe objects. Use `.data` to access
    the objects on this page, or use

    for item in list_object.auto_paging_iter():
      # do something with item

    to iterate over this and all following pages.
    """

    OBJECT_NAME = "list"
    data: List[T]
    next_page_url: Optional[str]

    def __getitem__(self, k):
        if isinstance(k, str):  # type: ignore
            return super(ListObject, self).__getitem__(k)
        else:
            raise KeyError(
                "You tried to access the %s index, but ListObjectV2 types only "
                "support string keys. (HINT: List calls return an object with "
                "a 'data' (which is the data array). You likely want to call "
                ".data[%s])" % (repr(k), repr(k))
            )

    def __iter__(self):
        return getattr(self, "data", []).__iter__()

    def __len__(self):
        return getattr(self, "data", []).__len__()

    def __reversed__(self):
        return getattr(self, "data", []).__reversed__()

    def auto_paging_iter(self):
        page = self.data
        next_page_url = self.next_page_url
        while True:
            for item in page:
                yield item
            if next_page_url is None:
                break

            result = self._request(
                "get",
                next_page_url,
                base_address="api",
            )
            assert isinstance(result, ListObject)
            page = result.data
            next_page_url = result.next_page_url
