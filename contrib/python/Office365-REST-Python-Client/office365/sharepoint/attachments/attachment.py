from typing import IO, AnyStr, Optional

from typing_extensions import Self

from office365.runtime.client_result import ClientResult
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.internal.queries.upload_file import create_upload_file_query
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath


class Attachment(Entity):
    """Represents an attachment file in a SharePoint List Item."""

    def __repr__(self):
        return self.server_relative_url or self.entity_type_name

    def download(self, file_object, use_path=True):
        """Download attachment file

        :type file_object: typing.IO
        :param bool use_path: Use Path instead of Url for addressing attachments
        """

        def _save_content(return_type):
            # type: (ClientResult[AnyStr]) -> None
            file_object.write(return_type.value)

        def _download_file_by_path():
            file = self.context.web.get_file_by_server_relative_path(
                str(self.server_relative_path)
            )
            file.get_content().after_execute(_save_content)

        def _download_file_by_url():
            file = self.context.web.get_file_by_server_relative_url(
                self.server_relative_url
            )
            file.get_content().after_execute(_save_content)

        if use_path:
            self.ensure_property("ServerRelativePath", _download_file_by_path)
        else:
            self.ensure_property("ServerRelativeUrl", _download_file_by_url)
        return self

    def get_content(self):
        # type: () -> ClientResult[AnyStr]
        """Gets the raw contents of attachment"""
        return_type = ClientResult(self.context)
        qry = FunctionQuery(self, "$value", None, return_type)
        self.context.add_query(qry)
        return return_type

    def recycle_object(self):
        """Move this attachment to site recycle bin."""
        qry = ServiceOperationQuery(self, "RecycleObject")
        self.context.add_query(qry)
        return self

    def upload(self, file_object, use_path=True):
        # type: (IO, bool) -> Self
        """
        Upload attachment into list item

        :type file_object: typing.IO
        :param bool use_path: Use Path instead of Url for addressing attachments
        """

        def _upload_file_by_url():
            target_file = self.context.web.get_file_by_server_relative_url(
                self.server_relative_url
            )
            qry = create_upload_file_query(target_file, file_object)
            self.context.add_query(qry)

        def _upload_file_by_path():
            target_file = self.context.web.get_file_by_server_relative_path(
                str(self.server_relative_path)
            )
            qry = create_upload_file_query(target_file, file_object)
            self.context.add_query(qry)

        if use_path:
            self.ensure_property("ServerRelativePath", _upload_file_by_path)
        else:
            self.ensure_property("ServerRelativeUrl", _upload_file_by_url)
        return self

    @property
    def file_name(self):
        # type: () -> Optional[str]
        """Specifies the file name of the list item attachment."""
        return self.properties.get("FileName", None)

    @property
    def file_name_as_path(self):
        """The file name of the attachment as a SP.ResourcePath."""
        return self.properties.get("FileNameAsPath", SPResPath())

    @property
    def server_relative_url(self):
        # type: () -> Optional[str]
        """The server-relative-url of the attachment"""
        return self.properties.get("ServerRelativeUrl", None)

    @property
    def server_relative_path(self):
        """The server-relative-path of the attachment."""
        return self.properties.get("ServerRelativePath", SPResPath())

    @property
    def property_ref_name(self):
        return "FileName"

    def set_property(self, name, value, persist_changes=True):
        super(Attachment, self).set_property(name, value, persist_changes)
        # fallback: create a new resource path
        if self._resource_path is None:
            if name == "ServerRelativeUrl":
                self._resource_path = self.context.web.get_file_by_server_relative_url(
                    value
                ).resource_path

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "FileNameAsPath": self.file_name_as_path,
                "ServerRelativePath": self.server_relative_path,
            }
            default_value = property_mapping.get(name, None)
        return super(Attachment, self).get_property(name, default_value)
