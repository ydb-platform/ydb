import base64

import requests

from office365.entity_collection import EntityCollection
from office365.outlook.mail.attachments.attachment import Attachment
from office365.runtime.client_result import ClientResult
from office365.runtime.compat import parse_query_string
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.odata.v4.upload_session import UploadSession
from office365.runtime.odata.v4.upload_session_request import UploadSessionRequest
from office365.runtime.queries.upload_session import UploadSessionQuery


class AttachmentCollection(EntityCollection[Attachment]):
    """Attachment collection"""

    def __init__(self, context, resource_path=None):
        super(AttachmentCollection, self).__init__(context, Attachment, resource_path)

    def add_file(self, name, content=None, content_type=None, base64_content=None):
        """
        Attach a file to message

        :param str name: The name representing the text that is displayed below the icon representing the
             embedded attachment
        :param str or None content: The contents of the file
        :param str or None content_type: The content type of the attachment.
        :param str or None base64_content: The contents of the file in the form of a base64 string.
        """
        if not content and not base64_content:
            raise TypeError("Either content or base64_content is required")
        from office365.outlook.mail.attachments.file import FileAttachment

        return_type = FileAttachment(self.context)
        return_type.name = name
        if base64_content:
            content = base64_content
        else:
            content = base64.b64encode(content.encode("utf-8")).decode("utf-8")
        return_type.content_bytes = content
        return_type.content_type = content_type
        self.add_child(return_type)
        return self

    def resumable_upload(self, source_path, chunk_size=1000000, chunk_uploaded=None):
        """
        Create an upload session to allow your app to upload files up to the maximum file size.
        An upload session allows your app to upload ranges of the file in sequential API requests,
        which allows the transfer to be resumed if a connection is dropped while the upload is in progress.

        :param str source_path: Local file path
        :param int chunk_size: File chunk size
        :param (int)->None chunk_uploaded: Upload action
        """
        from office365.outlook.mail.attachments.attachment_item import AttachmentItem
        from office365.outlook.mail.attachments.file import FileAttachment

        return_type = FileAttachment(self.context)
        self.add_child(return_type)

        qry = UploadSessionQuery(
            self, {"AttachmentItem": AttachmentItem.create_file(source_path)}
        )

        def _start_upload(result):
            # type: (ClientResult[UploadSession]) -> None
            with open(source_path, "rb") as local_file:
                session_request = UploadSessionRequest(
                    local_file, chunk_size, chunk_uploaded
                )

                def _construct_request(request):
                    # type: (RequestOptions) -> None
                    auth_token = parse_query_string(request.url, "authtoken")
                    request.set_header("Authorization", "Bearer {0}".format(auth_token))

                def _process_response(response):
                    # type: (requests.Response) -> None
                    location = response.headers.get("Location", None)
                    if location is None:
                        return
                    attachment_id = location[location.find("Attachments(") + 13 : -2]
                    return_type.set_property("id", attachment_id)

                session_request.beforeExecute += _construct_request
                session_request.afterExecute += _process_response
                session_request.execute_query(qry)

        self.context.add_query(qry).after_query_execute(
            _start_upload, execute_first=True
        )
        return self

    def create_upload_session(self, attachment_item):
        """
        Create an upload session that allows an app to iteratively upload ranges of a file,
             so as to attach the file to the specified Outlook item. The item can be a message or event.

        :type attachment_item: office365.mail.attachment_item.AttachmentItem
        """
        qry = UploadSessionQuery(self, {"AttachmentItem": attachment_item})
        self.context.add_query(qry)
        return qry.return_type
