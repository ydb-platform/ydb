from typing import TypeVar

from office365.runtime.client_object_collection import ClientObjectCollection

T = TypeVar("T")


class TaxonomyItemCollection(ClientObjectCollection[T]):
    pass
