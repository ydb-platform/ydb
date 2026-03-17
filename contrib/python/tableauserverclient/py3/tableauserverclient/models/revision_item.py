from datetime import datetime
from typing import Optional

from defusedxml.ElementTree import fromstring

from tableauserverclient.datetime_helpers import parse_datetime


class RevisionItem:
    def __init__(self):
        self._resource_id: Optional[str] = None
        self._resource_name: Optional[str] = None
        self._revision_number: Optional[str] = None
        self._current: Optional[bool] = None
        self._deleted: Optional[bool] = None
        self._created_at: Optional[datetime] = None
        self._user_id: Optional[str] = None
        self._user_name: Optional[str] = None

    @property
    def resource_id(self) -> Optional[str]:
        return self._resource_id

    @property
    def resource_name(self) -> Optional[str]:
        return self._resource_name

    @property
    def revision_number(self) -> Optional[str]:
        return self._revision_number

    @property
    def current(self) -> Optional[bool]:
        return self._current

    @property
    def deleted(self) -> Optional[bool]:
        return self._deleted

    @property
    def created_at(self) -> Optional[datetime]:
        return self._created_at

    @property
    def user_id(self) -> Optional[str]:
        return self._user_id

    @property
    def user_name(self) -> Optional[str]:
        return self._user_name

    def __repr__(self):
        return (
            "<RevisionItem# revisionNumber={_revision_number} "
            "current={_current} deleted={_deleted} user={_user_id}>".format(**self.__dict__)
        )

    @classmethod
    def from_response(cls, resp: bytes, ns, resource_item) -> list["RevisionItem"]:
        all_revision_items = list()
        parsed_response = fromstring(resp)
        all_revision_xml = parsed_response.findall(".//t:revision", namespaces=ns)
        for revision_xml in all_revision_xml:
            revision_item = cls()
            revision_item._resource_id = resource_item.id
            revision_item._resource_name = resource_item.name
            revision_item._revision_number = revision_xml.get("revisionNumber", None)
            revision_item._current = string_to_bool(revision_xml.get("current", ""))
            revision_item._deleted = string_to_bool(revision_xml.get("deleted", ""))
            revision_item._created_at = parse_datetime(revision_xml.get("publishedAt", None))
            for user in revision_xml.findall(".//t:publisher", namespaces=ns):
                revision_item._user_id = user.get("id", None)
                revision_item._user_name = user.get("name", None)

            all_revision_items.append(revision_item)
        return all_revision_items


# Used to convert string represented boolean to a boolean type
def string_to_bool(s: str) -> bool:
    return s.lower() == "true"
