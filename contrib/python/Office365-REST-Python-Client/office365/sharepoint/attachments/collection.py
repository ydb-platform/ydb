import os
from functools import partial
from typing import IO, AnyStr, Callable, Optional

from typing_extensions import Self

from office365.runtime.client_result import ClientResult
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.attachments.attachment import Attachment
from office365.sharepoint.attachments.creation_information import (
    AttachmentCreationInformation,
)
from office365.sharepoint.entity_collection import EntityCollection


class AttachmentCollection(EntityCollection[Attachment]):
    """Represents a collection of Attachment resources."""

    def __init__(self, context, resource_path=None, parent=None):
        super(AttachmentCollection, self).__init__(
            context, Attachment, resource_path, parent
        )

    def add(self, attachment_file_information):
        # type: (AttachmentCreationInformation|dict) -> Attachment
        """
        Adds the attachment represented by the file name and stream in the specified parameter to the list item.

        :param AttachmentCreationInformation attachment_file_information: The creation information which contains file
            name and content stream.
        """
        if isinstance(attachment_file_information, dict):
            attachment_file_information = AttachmentCreationInformation(
                attachment_file_information.get("filename"),
                attachment_file_information.get("content"),
            )

        return_type = Attachment(self.context)
        self.add_child(return_type)
        qry = ServiceOperationQuery(
            self,
            "add",
            {
                "filename": attachment_file_information.filename,
            },
            attachment_file_information.content,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def add_using_path(self, decoded_url, content_stream):
        # type: (str, AnyStr) -> Attachment
        """
        Adds the attachment represented by the file name and stream in the specified parameter to the list item.

        :param str decoded_url: Specifies the path for the attachment file.
        :param str content_stream: Stream containing the content of the attachment.
        """
        return_type = Attachment(self.context)
        params = {"DecodedUrl": decoded_url}
        qry = ServiceOperationQuery(
            self, "AddUsingPath", params, content_stream, None, return_type
        )
        self.context.add_query(qry)
        self.add_child(return_type)
        return return_type

    def delete_all(self):
        """Deletes all attachments"""

        def _delete_all(return_type):
            # type: ("AttachmentCollection") -> None
            [a.delete_object() for a in return_type]

        self.get().after_execute(_delete_all)
        return self

    def download(self, download_file, file_downloaded=None):
        # type: (IO, Optional[Callable[[Attachment], None]]) -> Self
        """Downloads attachments as a zip file"""
        import zipfile

        def _file_downloaded(attachment_file, result):
            # type: (Attachment, ClientResult[AnyStr]) -> None
            with zipfile.ZipFile(download_file.name, "a", zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(attachment_file.file_name, result.value)
                if callable(file_downloaded):
                    file_downloaded(attachment_file)

        def _download(return_type):
            for attachment_file in return_type:
                attachment_file.get_content().after_execute(
                    partial(_file_downloaded, attachment_file)
                )

        self.get().after_execute(_download)
        return self

    def upload(self, file, use_path=True):
        # type: (IO, bool) -> Attachment
        """Uploads the attachment"""
        info = AttachmentCreationInformation(os.path.basename(file.name), file.read())
        if use_path:
            return self.add_using_path(info.filename, info.content)
        else:
            return self.add(info)

    def get_by_filename(self, filename):
        """Retrieve Attachment file object by filename

        :param str filename: The specified file name.
        """
        return Attachment(
            self.context,
            ServiceOperationPath("GetByFileName", [filename], self.resource_path),
        )

    def get_by_filename_as_path(self, decoded_url):
        """Get the attachment file.

        :param str decoded_url: The specified file name.
        """
        return Attachment(
            self.context,
            ServiceOperationPath(
                "GetByFileNameAsPath", [decoded_url], self.resource_path
            ),
        )
