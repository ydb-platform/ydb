from datetime import datetime
from typing import IO, AnyStr, Optional

from typing_extensions import Self

from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.v3.entity import EntityPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity


class FileVersion(Entity):
    """Represents a version of a File object."""

    def __str__(self):
        return self.version_label

    def __repr__(self):
        return "Is Current: {0}, {1}".format(
            self.is_current_version, self.version_label
        )

    def download(self, file_object):
        # type: (IO) -> Self
        """Downloads the file version as a stream and save into a file."""

        def _save_file(return_type):
            # type: (ClientResult[AnyStr]) -> None
            file_object.write(return_type.value)

        def _file_version_loaded():
            self.open_binary_stream().after_execute(_save_file)

        self.ensure_property("ID", _file_version_loaded)
        return self

    def open_binary_stream(self):
        # type: () -> ClientResult[AnyStr]
        """Opens the file as a stream."""
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "OpenBinaryStream", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def open_binary_stream_with_options(self, open_options):
        # type: (int) -> ClientResult[AnyStr]
        """Opens the file as a stream."""
        return_type = ClientResult(self.context)
        qry = ServiceOperationQuery(
            self, "OpenBinaryStreamWithOptions", [open_options], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    @property
    def created(self):
        # type: () -> Optional[datetime]
        """Specifies the creation date and time for the file version."""
        return self.properties.get("Created", datetime.min)

    @property
    def created_by(self):
        """Gets the user that created the file version."""
        from office365.sharepoint.principal.users.user import User

        return self.properties.get(
            "CreatedBy",
            User(self.context, ResourcePath("CreatedBy", self.resource_path)),
        )

    @property
    def id(self):
        # type: () -> Optional[int]
        """Gets a file version identifier"""
        return int(self.properties.get("ID", -1))

    @property
    def url(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the relative URL of the file version based on the URL for the site or subsite."""
        return self.properties.get("Url", None)

    @property
    def version_label(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the implementation specific identifier of the file."""
        return self.properties.get("VersionLabel", None)

    @property
    def is_current_version(self):
        # type: () -> Optional[bool]
        """Gets a value that specifies whether the file version is the current version."""
        return self.properties.get("IsCurrentVersion", None)

    @property
    def checkin_comment(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the check-in comment."""
        return self.properties.get("CheckInComment", None)

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "CreatedBy": self.created_by,
            }
            default_value = property_mapping.get(name, None)
        return super(FileVersion, self).get_property(name, default_value)

    def set_property(self, key, value, persist_changes=True):
        super(FileVersion, self).set_property(key, value, persist_changes)
        if key.lower() == self.property_ref_name.lower():
            if self._resource_path is None:
                self._resource_path = EntityPath(
                    value, self.parent_collection.resource_path
                )
            else:
                self._resource_path.patch(value)
        return self
