import datetime
from typing import IO, AnyStr, Optional

from typing_extensions import Self

from office365.entity import Entity
from office365.runtime.client_result import ClientResult
from office365.runtime.queries.function import FunctionQuery


class Attachment(Entity):
    """A file or item (contact, event or message) attached to an event or message."""

    def __repr__(self):
        return self.name or self.id

    def download(self, file_object):
        # type: (IO) -> Self
        """Downloads raw contents of a file or item attachment"""

        def _save_content(return_type):
            # type: (ClientResult[AnyStr]) -> None
            file_object.write(return_type.value)

        self.get_content().after_execute(_save_content)
        return self

    def get_content(self):
        # type: () -> ClientResult[AnyStr]
        """Gets the raw contents of a file or item attachment"""
        return_type = ClientResult(self.context)
        qry = FunctionQuery(self, "$value", None, return_type)
        self.context.add_query(qry)
        return return_type

    @property
    def name(self):
        # type: () -> Optional[str]
        """The attachment's file name."""
        return self.properties.get("name", None)

    @name.setter
    def name(self, value):
        # type: (str) -> None
        """Sets the attachment's file name."""
        self.set_property("name", value)

    @property
    def content_type(self):
        # type: () -> Optional[str]
        return self.properties.get("contentType", None)

    @content_type.setter
    def content_type(self, value):
        # type: (str) -> None
        self.set_property("contentType", value)

    @property
    def size(self):
        # type: () -> Optional[int]
        return self.properties.get("size", None)

    @property
    def last_modified_datetime(self):
        # type: () -> Optional[datetime.datetime]
        """The Timestamp type represents date and time information using ISO 8601 format and is always in UTC time."""
        return self.properties.get("lastModifiedDateTime", datetime.datetime.min)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "lastModifiedDateTime": self.last_modified_datetime,
            }
            default_value = property_mapping.get(name, None)
        return super(Attachment, self).get_property(name, default_value)
