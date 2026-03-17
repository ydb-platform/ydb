from datetime import datetime
from typing import Optional
from xml.etree.ElementTree import Element

from defusedxml.ElementTree import fromstring
from typing_extensions import Self

from tableauserverclient.datetime_helpers import parse_datetime
from tableauserverclient.models.user_item import UserItem


class CollectionItem:
    def __init__(self) -> None:
        self.id: Optional[str] = None
        self.name: Optional[str] = None
        self.description: Optional[str] = None
        self.created_at: Optional[datetime] = None
        self.updated_at: Optional[datetime] = None
        self.owner: Optional[UserItem] = None
        self.total_item_count: Optional[int] = None
        self.permissioned_item_count: Optional[int] = None
        self.visibility: Optional[str] = None  # Assuming visibility is a string, adjust as necessary

    @classmethod
    def from_response(cls, response: bytes, ns) -> list[Self]:
        parsed_response = fromstring(response)

        collection_elements = parsed_response.findall(".//t:collection", namespaces=ns)
        if not collection_elements:
            raise ValueError("No collection element found in the response")

        collections = [cls.from_xml(c, ns) for c in collection_elements]
        return collections

    @classmethod
    def from_xml(cls, xml: Element, ns) -> Self:
        collection_item = cls()
        collection_item.id = xml.get("id")
        collection_item.name = xml.get("name")
        collection_item.description = xml.get("description")
        collection_item.created_at = parse_datetime(xml.get("createdAt"))
        collection_item.updated_at = parse_datetime(xml.get("updatedAt"))
        owner_element = xml.find(".//t:owner", namespaces=ns)
        if owner_element is not None:
            collection_item.owner = UserItem.from_xml(owner_element, ns)
        else:
            collection_item.owner = None
        collection_item.total_item_count = int(xml.get("totalItemCount", 0))
        collection_item.permissioned_item_count = int(xml.get("permissionedItemCount", 0))
        collection_item.visibility = xml.get("visibility")

        return collection_item
