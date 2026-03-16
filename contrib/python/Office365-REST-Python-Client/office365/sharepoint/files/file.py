import datetime
from typing import TYPE_CHECKING, AnyStr
from urllib.parse import quote, unquote

import requests

from office365.runtime.client_result import ClientResult
from office365.runtime.http.http_method import HttpMethod
from office365.runtime.http.request_options import RequestOptions
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.function import FunctionQuery
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.runtime.queries.update_entity import UpdateEntityQuery
from office365.sharepoint.activities.capabilities import ActivityCapabilities
from office365.sharepoint.entity import Entity
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.files.versions.collection import FileVersionCollection
from office365.sharepoint.files.versions.event import FileVersionEvent
from office365.sharepoint.folders.folder import Folder
from office365.sharepoint.listitems.listitem import ListItem
from office365.sharepoint.permissions.irm.effective_settings import (
    EffectiveInformationRightsManagementSettings,
)
from office365.sharepoint.permissions.irm.file_settings import (
    InformationRightsManagementFileSettings,
)
from office365.sharepoint.principal.users.user import User
from office365.sharepoint.sharing.links.share_response import ShareLinkResponse
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath
from office365.sharepoint.utilities.upload_status import UploadStatus
from office365.sharepoint.utilities.wopi_frame_action import SPWOPIFrameAction
from office365.sharepoint.webparts.limited_manager import LimitedWebPartManager
from office365.sharepoint.webparts.personalization_scope import PersonalizationScope

if TYPE_CHECKING:
    from typing import Optional


class AbstractFile(Entity):
    def read(self):
        """Immediately read content of file"""
        if not self.is_property_available("ServerRelativeUrl"):
            raise ValueError
        response = File.open_binary(self.context, self.properties["ServerRelativeUrl"])
        return response.content

    def write(self, content):
        """Immediately writes content of file"""
        if not self.is_property_available("ServerRelativeUrl"):
            raise ValueError
        response = File.save_binary(
            self.context, self.properties["ServerRelativeUrl"], content
        )
        return response


class File(AbstractFile):
    """Represents a file in a SharePoint Web site that can be a Web Part Page, an item in a document library,
    or a file in a folder."""

    def __repr__(self):
        return self.serverRelativeUrl or self.unique_id or self.entity_type_name

    def __str__(self):
        return self.name or self.entity_type_name

    @staticmethod
    def from_url(abs_url):
        """
        Retrieves a File from absolute url
        :type abs_url: str
        """
        from office365.sharepoint.client_context import ClientContext

        ctx = ClientContext.from_url(abs_url)
        file_relative_url = abs_url.replace(ctx.base_url, "")
        return_type = ctx.web.get_file_by_server_relative_url(file_relative_url)
        return return_type

    def create_anonymous_link(self, is_edit_link=False):
        """Create an anonymous link which can be used to access a document without needing to authenticate.

        :param bool is_edit_link: If true, the link will allow the guest user edit privileges on the item.
        """
        return_type = ClientResult(self.context, str())

        def _file_loaded():
            from office365.sharepoint.webs.web import Web

            Web.create_anonymous_link(
                self.context, self.serverRelativeUrl, is_edit_link, return_type
            )

        self.ensure_property("ServerRelativeUrl", _file_loaded)
        return return_type

    def create_anonymous_link_with_expiration(self, expiration, is_edit_link=False):
        # type: (datetime.datetime, bool) -> ClientResult[str]
        """Creates and returns an anonymous link that can be used to access a document without needing to authenticate.

        :param is_edit_link: If true, the link will allow the guest user edit privileges on the item.
        string parameters
        :param expiration: A date/time string for which the format conforms to the ISO 8601:2004(E) complete
        representation for calendar date and time of day, and which represents the time and date of expiry for the
        anonymous link. Both the minutes and hour value MUST be specified for the difference between the local and
        UTC time. Midnight is represented as 00:00:00.
        """
        return_type = ClientResult(self.context, str())

        def _file_loaded():
            from office365.sharepoint.webs.web import Web

            Web.create_anonymous_link_with_expiration(
                self.context,
                self.serverRelativeUrl,
                is_edit_link,
                expiration.isoformat(timespec="seconds"),
                return_type,
            )

        self.ensure_property("ServerRelativeUrl", _file_loaded)
        return return_type

    def get_content(self):
        # type: () -> ClientResult[AnyStr]
        """Downloads a file content"""
        return_type = ClientResult(self.context)
        qry = FunctionQuery(self, "$value", return_type=return_type)
        self.context.add_query(qry)
        return return_type

    def get_pre_authorized_access_url(self, expiration_hours):
        # type: (int) -> ClientResult[str]
        """Returns a link for downloading the file without authentication.
        :param int expiration_hours: The number of hours until the link expires. If the maximum expiration time
        defined in the web application is less than the specified expiration time, the maximum expiration time
        takes precedence.
        """
        return_type = ClientResult(self.context, str())
        params = {"expirationHours": expiration_hours}
        qry = FunctionQuery(self, "GetPreAuthorizedAccessUrl", params, return_type)
        self.context.add_query(qry)
        return return_type

    def get_absolute_url(self):
        # type: () -> ClientResult[str]
        """Gets absolute url of a File"""
        return_type = ClientResult(self.context)

        def _loaded():
            return_type.set_property(
                "__value", self.listItemAllFields.properties.get("EncodedAbsUrl")
            )

        self.listItemAllFields.ensure_property("EncodedAbsUrl", _loaded)
        return return_type

    def get_sharing_information(self):
        """Gets the sharing information for a file."""
        return self.listItemAllFields.get_sharing_information()

    def get_wopi_frame_url(self, action=SPWOPIFrameAction.View):
        """
        Returns the full URL to the SharePoint frame page that will initiate the specified WOPI frame action with the
        file's associated WOPI application. If there is no associated WOPI application or associated action,
        the return value is an empty string.

        :param str action: The full URL to the WOPI frame.
        """
        return_type = ClientResult(self.context, str())
        params = {"action": action}
        qry = ServiceOperationQuery(
            self, "GetWOPIFrameUrl", params, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def share_link(self, link_kind, expiration=None, role=None, password=None):
        # type: (int, Optional[datetime.datetime], Optional[int], Optional[str]) -> ClientResult[ShareLinkResponse]
        """Creates a tokenized sharing link for a file based on the specified parameters and optionally
        sends an email to the people that are listed in the specified parameters.

        :param link_kind: The kind of the tokenized sharing link to be created/updated or retrieved.
        :param expiration: A date/time string for which the format conforms to the ISO 8601:2004(E)
            complete representation for calendar date and time of day and which represents the time and date of expiry
            for the tokenized sharing link. Both the minutes and hour value MUST be specified for the difference
            between the local and UTC time. Midnight is represented as 00:00:00. A null value indicates no expiry.
            This value is only applicable to tokenized sharing links that are anonymous access links.
        :param role: The role to be used for the tokenized sharing link. This is required for Flexible links
            and ignored for all other kinds.
        :param password: Optional password value to apply to the tokenized sharing link,
            if it can support password protection.
        """
        return self.listItemAllFields.share_link(link_kind, expiration, role, password)

    def unshare_link(self, link_kind, share_id=None):
        """
        Removes the specified tokenized sharing link of the file.

        :param int link_kind: This optional value specifies the globally unique identifier (GUID) of the tokenized
            sharing link that is intended to be removed.
        :param str or None share_id: The kind of tokenized sharing link that is intended to be removed.
        """
        return self.listItemAllFields.unshare_link(link_kind, share_id)

    def get_image_preview_uri(self, width, height, client_type=None):
        """
        Returns the uri where the thumbnail with the closest size to the desired can be found.
        The actual resolution of the thumbnail might not be the same as the desired values.


        :param int width: The desired width of the resolution.
        :param int height: The desired height of the resolution.
        :param str client_type: The client type. Used for logging.
        """
        return_type = ClientResult(self.context, str())
        payload = {"width": width, "height": height, "clientType": client_type}
        qry = ServiceOperationQuery(
            self, "GetImagePreviewUri", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def get_image_preview_url(self, width, height, client_type=None):
        """
        Returns the url where the thumbnail with the closest size to the desired can be found.
        The actual resolution of the thumbnail might not be the same as the desired values.

        :param int width: The desired width of the resolution.
        :param int height: The desired height of the resolution.
        :param str client_type: The client type. Used for logging.
        """
        return_type = ClientResult(self.context, str())
        payload = {"width": width, "height": height, "clientType": client_type}
        qry = ServiceOperationQuery(
            self, "GetImagePreviewUrl", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def recycle(self):
        """Moves the file to the Recycle Bin and returns the identifier of the new Recycle Bin item."""
        return_type = ClientResult(self.context, str())
        qry = ServiceOperationQuery(self, "Recycle", None, None, None, return_type)
        self.context.add_query(qry)
        return return_type

    def approve(self, comment):
        """
        Approves the file submitted for content approval with the specified comment.

        :param str comment: A string containing the comment.
        """
        qry = ServiceOperationQuery(self, "Approve", {"comment": comment})
        self.context.add_query(qry)
        return self

    def deny(self, comment):
        """Denies approval for a file that was submitted for content approval.

        :param str comment: A string containing the comment.
        """
        qry = ServiceOperationQuery(self, "Deny", {"comment": comment})
        self.context.add_query(qry)
        return self

    def copyto(self, destination, overwrite=False, file_name=None):
        # type: (Folder|str, bool, str) -> "File"
        """Copies the file to the destination URL.

        :param office365.sharepoint.folders.folder.Folder or str destination: Specifies the destination folder or
            folder server relative url where to copy a file.
        :param bool overwrite: Specifies whether a file with the same name is overwritten.
        :param str file_name: A new file name
        """
        return_type = File(self.context)
        self.parent_collection.add_child(return_type)

        def _copyto(destination_folder):
            # type: (Folder) -> None
            file_path = "/".join(
                [str(destination_folder.serverRelativeUrl), file_name or self.name]
            )
            return_type.set_property("ServerRelativeUrl", file_path)

            params = {"strNewUrl": file_path, "boverwrite": overwrite}
            qry = ServiceOperationQuery(self, "CopyTo", params)
            self.context.add_query(qry)

        def _source_file_resolved():
            if isinstance(destination, Folder):
                destination.ensure_property("ServerRelativeUrl", _copyto, destination)
            else:
                self.context.web.ensure_folder_path(destination).after_execute(_copyto)

        self.ensure_properties(["ServerRelativeUrl", "Name"], _source_file_resolved)
        return return_type

    def copyto_using_path(self, destination, overwrite=False, file_name=None):
        """
        Copies the file to the destination path. Server MUST overwrite an existing file of the same name
        if overwrite is true.

        :param bool overwrite: Specifies whether a file with the same name is overwritten.
        :param office365.sharepoint.folders.folder.Folder or str destination: Specifies the destination folder or
            folder server relative url where to copy a file.
        :param str file_name: New file name
        """

        return_type = File(self.context)
        self.parent_collection.add_child(return_type)

        def _copyto_using_path(destination_folder):
            # type: (Folder) -> None
            file_path = "/".join(
                [str(destination_folder.server_relative_path), file_name or self.name]
            )
            return_type.set_property("ServerRelativePath", file_path)

            params = {"DecodedUrl": file_path, "bOverWrite": overwrite}
            qry = ServiceOperationQuery(self, "CopyToUsingPath", params)
            self.context.add_query(qry)

        def _source_file_resolved():
            if isinstance(destination, Folder):
                destination.ensure_property(
                    "ServerRelativePath", _copyto_using_path, destination
                )
            else:
                self.context.web.ensure_folder_path(destination).get().select(
                    ["ServerRelativePath"]
                ).after_execute(_copyto_using_path)

        self.ensure_properties(["ServerRelativePath", "Name"], _source_file_resolved)
        return return_type

    def moveto(self, destination, flag):
        """Moves the file to the specified destination url.

        :param str or office365.sharepoint.folders.folder.Folder destination: Specifies the existing folder or folder
             site relative url.
        :param int flag: Specifies the kind of move operation.
        """

        def _moveto(destination_folder):
            # type: (Folder) -> None
            file_url = "/".join([str(destination_folder.serverRelativeUrl), self.name])

            params = {"newurl": file_url, "flags": flag}
            qry = ServiceOperationQuery(self, "moveto", params)
            self.context.add_query(qry)

            def _update_file(return_type):
                self.set_property("ServerRelativeUrl", file_url)

            self.context.after_query_execute(_update_file)

        def _source_file_resolved():
            if isinstance(destination, Folder):
                destination.ensure_property("ServerRelativeUrl", _moveto, destination)
            else:
                self.context.web.ensure_folder_path(destination).get().after_execute(
                    _moveto
                )

        self.ensure_properties(["ServerRelativeUrl", "Name"], _source_file_resolved)
        return self

    def move_to_using_path(self, destination, flag):
        """
        Moves the file to the specified destination path.

        :param str or office365.sharepoint.folders.folder.Folder destination: Specifies the destination folder path or
            existing folder object
        :param int flag: Specifies the kind of move operation.
        """

        def _move_to_using_path(destination_folder):
            # type: (Folder) -> None
            file_path = "/".join(
                [str(destination_folder.server_relative_path), self.name]
            )
            params = {"DecodedUrl": file_path, "moveOperations": flag}
            qry = ServiceOperationQuery(self, "MoveToUsingPath", params)

            def _update_file(return_type):
                # type: (File) -> None
                self.set_property("ServerRelativePath", file_path)

            self.context.add_query(qry).after_query_execute(_update_file)

        def _source_file_resolved():
            if isinstance(destination, Folder):
                destination.ensure_property(
                    "ServerRelativePath", _move_to_using_path, destination
                )
            else:
                self.context.web.ensure_folder_path(destination).get().select(
                    ["ServerRelativePath"]
                ).after_execute(_move_to_using_path)

        self.ensure_properties(["ServerRelativePath", "Name"], _source_file_resolved)
        return self

    def publish(self, comment):
        """Submits the file for content approval with the specified comment.
        :param str comment: Specifies the comment.
        """
        qry = ServiceOperationQuery(self, "Publish", {"comment": comment})
        self.context.add_query(qry)
        return self

    def unpublish(self, comment):
        """Removes the file from content approval or unpublishes a major version.
        :param str comment: Specifies the comment for UnPublish. Its length MUST be equal to or less than 1023.
        """
        qry = ServiceOperationQuery(self, "unpublish", {"comment": comment})
        self.context.add_query(qry)
        return self

    def check_access_and_post_view_audit_event(self):
        """"""
        return_type = ClientResult(self.context, bool())
        qry = ServiceOperationQuery(
            self, "CheckAccessAndPostViewAuditEvent", return_type=return_type
        )
        self.context.add_query(qry)
        return return_type

    def checkout(self):
        """Checks out the file from a document library based on the check-out type."""
        qry = ServiceOperationQuery(self, "checkout")
        self.context.add_query(qry)
        return self

    def checkin(self, comment, checkin_type):
        """
        Checks the file in to a document library based on the check-in type.

        :param comment: comment to the new version of the file
        :param checkin_type: 0 (minor), or 1 (major) or 2 (overwrite)
            For more information on checkin types, please see
            https://docs.microsoft.com/en-us/previous-versions/office/sharepoint-csom/ee542953(v%3Doffice.15)
        :param int checkin_type: Specifies the type of check-in.
        """
        params = {"comment": comment, "checkInType": checkin_type}
        qry = ServiceOperationQuery(self, "checkin", params)
        self.context.add_query(qry)
        return self

    def undocheckout(self):
        """Reverts an existing checkout for the file."""
        qry = ServiceOperationQuery(self, "UndoCheckout")
        self.context.add_query(qry)
        return self

    def get_limited_webpart_manager(self, scope=PersonalizationScope.User):
        """Specifies the control set used to access, modify, or add Web Parts associated with this Web Part Page and
        view.

        :param int scope:  Specifies the personalization scope value that depicts how Web Parts are viewed on the
            Web Part Page.
        """
        return LimitedWebPartManager(
            self.context,
            ServiceOperationPath(
                "GetLimitedWebPartManager", [scope], self.resource_path
            ),
        )

    def open_binary_stream(self):
        """Opens the file as a stream."""
        return_type = ClientResult(self.context, bytes())
        qry = ServiceOperationQuery(
            self, "OpenBinaryStream", None, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def save_binary_stream(self, stream):
        """Saves the file in binary format.

        :param str or bytes stream: A stream containing the contents of the specified file.
        """
        qry = ServiceOperationQuery(self, "SaveBinaryStream", None, stream)
        self.context.add_query(qry)
        return self

    def get_upload_status(self, upload_id):
        """Gets the status of a chunk upload session.
        :param str upload_id:  The upload session ID.
        """
        payload = {
            "uploadId": upload_id,
        }
        return_type = UploadStatus(self.context)
        qry = ServiceOperationQuery(
            self, "GetUploadStatus", None, payload, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def upload_with_checksum(self, upload_id, checksum, stream):
        # type: (str, str, bytes) -> File
        """
        :param str upload_id: The upload session ID.
        :param str checksum:
        :param bytes stream:
        """
        return_type = File(self.context)
        params = {"uploadId": upload_id, "checksum": checksum}
        qry = ServiceOperationQuery(
            self, "UploadWithChecksum", params, stream, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def cancel_upload(self, upload_id):
        """
        Aborts the chunk upload session without saving the uploaded data. If StartUpload (section 3.2.5.64.2.1.22)
        created the file, the file will be deleted.

        :param str upload_id:  The upload session ID.
        """
        payload = {
            "uploadId": upload_id,
        }
        qry = ServiceOperationQuery(self, "CancelUpload", None, payload)
        self.context.add_query(qry)
        return self

    def start_upload(self, upload_id, content):
        """Starts a new chunk upload session and uploads the first fragment.

        :param bytes content: File content
        :param str upload_id: Upload session id
        """
        return_type = ClientResult(self.context, int())
        params = {"uploadID": upload_id}
        qry = ServiceOperationQuery(
            self, "startUpload", params, content, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def continue_upload(self, upload_id, file_offset, content):
        """
        Continues the chunk upload session with an additional fragment. The current file content is not changed.

        :param str upload_id: Upload session id
        :param int file_offset: File offset
        :param bytes content: File content
        """
        return_type = ClientResult(self.context, int())
        qry = ServiceOperationQuery(
            self,
            "continueUpload",
            {
                "uploadID": upload_id,
                "fileOffset": file_offset,
            },
            content,
            None,
            return_type,
        )
        self.context.add_query(qry)
        return return_type

    def finish_upload(self, upload_id, file_offset, content):
        """Uploads the last file fragment and commits the file. The current file content is changed when this method
        completes.

        :param str upload_id: Upload session id
        :param int file_offset: File offset
        :param bytes content: File content
        """
        params = {"uploadID": upload_id, "fileOffset": file_offset}
        qry = ServiceOperationQuery(self, "finishUpload", params, content, None, self)
        self.context.add_query(qry)
        return self

    def finish_upload_with_checksum(self, upload_id, file_offset, checksum, stream):
        """Uploads the last file fragment and commits the file. The current file content is changed when this method
        completes.

        :param str upload_id: Upload session id
        :param int file_offset: File offset
        :param str checksum: File checksum
        :param bytes stream: File content
        """
        payload = {
            "uploadID": upload_id,
            "fileOffset": file_offset,
            "checksum": checksum,
        }
        qry = ServiceOperationQuery(
            self, "FinishUploadWithChecksum", payload, stream, None, self
        )
        self.context.add_query(qry)
        return self

    @staticmethod
    def save_binary(context, server_relative_url, content):
        """Uploads a file

        :type context: office365.sharepoint.client_context.ClientContext
        :type server_relative_url: str
        :type content: str
        """
        try:
            decoded_server_relative_url = unquote(server_relative_url)
        except (ValueError, AttributeError, TypeError):
            decoded_server_relative_url = server_relative_url
        url = quote(
            r"{0}/web/getFileByServerRelativePath(DecodedUrl='{1}')/\$value".format(
                context.service_root_url(), decoded_server_relative_url
            ),
            safe=":/",
        )
        request = RequestOptions(url)
        request.method = HttpMethod.Post
        request.set_header("X-HTTP-Method", "PUT")
        request.data = content
        response = context.pending_request().execute_request_direct(request)
        return response

    @staticmethod
    def open_binary(context, server_relative_url):
        """
        Returns the file object located at the specified server-relative URL.

        :type context: office365.sharepoint.client_context.ClientContext
        :type server_relative_url: str
        :return Response
        """
        try:
            decoded_server_relative_url = unquote(server_relative_url)
        except (ValueError, AttributeError, TypeError):
            decoded_server_relative_url = server_relative_url
        url = quote(
            r"{0}/web/getFileByServerRelativePath(DecodedUrl='{1}')/\$value".format(
                context.service_root_url(), decoded_server_relative_url
            ),
            safe=":/",
        )
        request = RequestOptions(url)
        request.method = HttpMethod.Get
        response = context.pending_request().execute_request_direct(request)
        return response

    def download(self, file_object, after_downloaded=None):
        """
        Download a file content. Use this method to download a content of a small size

        :param typing.IO file_object: File object
        :param (File) -> None after_downloaded: A download callback
        """

        def _save_content(return_type):
            # type: (ClientResult[AnyStr]) -> None
            file_object.write(return_type.value)
            if callable(after_downloaded):
                after_downloaded(self)

        def _download_inner():
            self.get_content().after_execute(_save_content)

        self.ensure_property("ServerRelativePath", _download_inner)
        return self

    def download_session(
        self, file_object, chunk_downloaded=None, chunk_size=1024 * 1024, use_path=True
    ):
        """
        Download a file content. Use this method to download a content of a large size

        :type file_object: typing.IO
        :type chunk_downloaded: (int)->None or None
        :type chunk_size: int
        :param bool use_path: File addressing by path flag
        """

        def _download_as_stream():
            qry = ServiceOperationQuery(self, "$value")

            def _construct_request(request):
                # type: (RequestOptions) -> None
                request.stream = True
                request.method = HttpMethod.Get

            def _process_response(response):
                # type: (requests.Response) -> None
                response.raise_for_status()
                bytes_read = 0
                for chunk in response.iter_content(chunk_size=chunk_size):
                    bytes_read += len(chunk)
                    if callable(chunk_downloaded):
                        chunk_downloaded(bytes_read)
                    file_object.write(chunk)

            self.context.add_query(qry).before_query_execute(
                _construct_request
            ).after_execute(_process_response)

        if use_path:
            self.ensure_property("ServerRelativePath", _download_as_stream)
        else:
            self.ensure_property("ServerRelativeUrl", _download_as_stream)
        return self

    def rename(self, new_file_name):
        """
        Rename a file
        :param str new_file_name: A new file name
        """
        item = self.listItemAllFields
        item.set_property("FileLeafRef", new_file_name)
        qry = UpdateEntityQuery(item)
        self.context.add_query(qry)
        return self

    @property
    def activity_capabilities(self):
        """"""
        return self.properties.get("ActivityCapabilities", ActivityCapabilities())

    @property
    def checkin_comment(self):
        # type: () -> Optional[str]
        """Specifies the comment used when a document is checked into a document library.
        Its length MUST be equal to or less than 1023."""
        return self.properties.get("CheckInComment", None)

    @property
    def content_tag(self):
        # type: () -> Optional[str]
        """Returns internal version of content, used to validate document equality for read purposes."""
        return self.properties.get("ContentTag", None)

    @property
    def author(self):
        """Specifies the user who added the file."""
        return self.properties.get(
            "Author", User(self.context, ResourcePath("Author", self.resource_path))
        )

    @property
    def checked_out_by_user(self):
        """Gets an object that represents the user who has checked out the file."""
        return self.properties.get(
            "CheckedOutByUser",
            User(self.context, ResourcePath("CheckedOutByUser", self.resource_path)),
        )

    @property
    def check_out_type(self):
        # type: () -> Optional[int]
        return self.properties.get("CheckOutType", None)

    @property
    def expiration_date(self):
        # type: () -> Optional[datetime.datetime]
        """Specifies the date and time when the file expires."""
        return self.properties.get("ExpirationDate", datetime.datetime.min)

    @property
    def version_events(self):
        # type: () -> EntityCollection[FileVersionEvent]
        """Gets the history of events on this version object."""
        return self.properties.get(
            "VersionEvents",
            EntityCollection(
                self.context,
                FileVersionEvent,
                ResourcePath("VersionEvents", self.resource_path),
            ),
        )

    @property
    def effective_information_rights_management_settings(self):
        """
        Returns the effective Information Rights Management (IRM) settings for the file.

        A file can be IRM-protected based on the IRM settings for the file itself, based on the IRM settings for the
        list which contains the file, or based on a rule. From greatest to least, IRM settings take precedence in the
        following order: rule, list, then file.
        """
        path = ResourcePath(
            "EffectiveInformationRightsManagementSettings", self.resource_path
        )
        return self.properties.get(
            "EffectiveInformationRightsManagementSettings",
            EffectiveInformationRightsManagementSettings(self.context, path),
        )

    @property
    def information_rights_management_settings(self):
        """Returns the Information Rights Management (IRM) settings for the file."""
        return self.properties.get(
            "InformationRightsManagementSettings",
            InformationRightsManagementFileSettings(
                self.context,
                ResourcePath("InformationRightsManagementSettings", self.resource_path),
            ),
        )

    @property
    def listItemAllFields(self):
        # type: () -> ListItem
        """Gets a value that specifies the list item fields values for the list item corresponding to the file."""
        return self.properties.setdefault(
            "ListItemAllFields",
            ListItem(
                self.context, ResourcePath("listItemAllFields", self.resource_path)
            ),
        )

    @property
    def version_expiration_report(self):
        """"""
        return self.properties.get(
            "VersionExpirationReport",
            FileVersionCollection(
                self.context,
                ResourcePath("VersionExpirationReport", self.resource_path),
            ),
        )

    @property
    def versions(self):
        # type: () -> FileVersionCollection
        """Gets a value that returns a collection of file version objects that represent the versions of the file."""
        return self.properties.get(
            "Versions",
            FileVersionCollection(
                self.context, ResourcePath("versions", self.resource_path)
            ),
        )

    @property
    def modified_by(self):
        """Gets a value that returns the user who last modified the file."""
        return self.properties.get(
            "ModifiedBy",
            User(self.context, ResourcePath("ModifiedBy", self.resource_path)),
        )

    @property
    def locked_by_user(self):
        """Gets a value that returns the user that owns the current lock on the file."""
        return self.properties.get(
            "LockedByUser",
            User(self.context, ResourcePath("LockedByUser", self.resource_path)),
        )

    @property
    def serverRelativeUrl(self):
        # type: () -> Optional[str]
        """Gets the relative URL of the file based on the URL for the server."""
        return self.properties.get("ServerRelativeUrl", None)

    @property
    def server_relative_path(self):
        """Gets the server-relative Path of the list folder."""
        return self.properties.get("ServerRelativePath", SPResPath())

    @property
    def length(self):
        # type: () -> Optional[int]
        """Gets the file size."""
        return int(self.properties.get("Length", 0))

    @property
    def exists(self):
        # type: () -> Optional[bool]
        """Specifies whether the file exists."""
        return self.properties.get("Exists", None)

    @property
    def irm_enabled(self):
        # type: () -> Optional[bool]
        """Specifies whether or not Information Rights Management (IRM) is enabled at the file level.
        A value of true indicates IRM is enabled; a value of false indicates IRM is disabled.
        """
        return self.properties.get("IrmEnabled", None)

    @property
    def level(self):
        # type: () -> Optional[str]
        """Specifies the publishing level of the file."""
        return self.properties.get("Level", None)

    @property
    def linking_uri(self):
        # type: () -> Optional[str]
        """Specifies the URL that is suitable for durable linking to the file."""
        return self.properties.get("LinkingUri", None)

    @property
    def name(self):
        # type: () -> Optional[str]
        """Specifies the file name including the extension.
        It MUST NOT be NULL. Its length MUST be equal to or less than 260.
        """
        return self.properties.get("Name", None)

    @property
    def list_id(self):
        # type: () -> Optional[str]
        """Gets the GUID that identifies the List containing the file."""
        return self.properties.get("ListId", None)

    @property
    def site_id(self):
        # type: () -> Optional[str]
        """Gets the GUID that identifies the site collection containing the file."""
        return self.properties.get("SiteId", None)

    @property
    def web_id(self):
        # type: () -> Optional[str]
        """Gets the GUID for the site containing the file."""
        return self.properties.get("WebId", None)

    @property
    def time_created(self):
        # type: () -> Optional[datetime.datetime]
        """Gets a value that specifies when the file was created."""
        return self.properties.get("TimeCreated", datetime.datetime.min)

    @property
    def time_last_modified(self):
        # type: () -> Optional[datetime.datetime]
        """Specifies when the file was last modified."""
        return self.properties.get("TimeLastModified", datetime.datetime.min)

    @property
    def minor_version(self):
        # type: () -> Optional[int]
        """Gets a value that specifies the minor version of the file."""
        return self.properties.get("MinorVersion", None)

    @property
    def major_version(self):
        # type: () -> Optional[int]
        """Gets a value that specifies the major version of the file."""
        return self.properties.get("MajorVersion", None)

    @property
    def unique_id(self):
        # type: () -> Optional[str]
        """Gets a value that specifies the a file unique identifier"""
        return self.properties.get("UniqueId", None)

    @property
    def customized_page_status(self):
        # type: () -> Optional[int]
        """Specifies the customization status of the file."""
        return self.properties.get("CustomizedPageStatus", None)

    @property
    def parent_folder(self):
        # type: () -> Optional[Folder]
        if self.parent_collection is None:
            return None
        return self.parent_collection.parent

    def get_property(self, name, default_value=None):
        if default_value is None:
            property_mapping = {
                "CheckedOutByUser": self.checked_out_by_user,
                "VersionEvents": self.version_events,
                "EffectiveInformationRightsManagementSettings": self.effective_information_rights_management_settings,
                "ExpirationDate": self.expiration_date,
                "InformationRightsManagementSettings": self.information_rights_management_settings,
                "LockedByUser": self.locked_by_user,
                "ModifiedBy": self.modified_by,
                "ServerRelativePath": self.server_relative_path,
                "TimeCreated": self.time_created,
                "TimeLastModified": self.time_last_modified,
                "VersionExpirationReport": self.version_expiration_report,
            }
            default_value = property_mapping.get(name, None)
        return super(File, self).get_property(name, default_value)

    def set_property(self, name, value, persist_changes=True):
        super(File, self).set_property(name, value, persist_changes)

        # prioritize using UniqueId
        if name == "UniqueId":
            self._resource_path = self.context.web.get_file_by_id(value).resource_path

        # fallback: create a new resource path
        if self._resource_path is None:
            if name == "ServerRelativeUrl":
                self._resource_path = self.context.web.get_file_by_server_relative_url(
                    value
                ).resource_path
            elif name == "ServerRelativePath":
                self._resource_path = self.context.web.get_file_by_server_relative_path(
                    value
                ).resource_path
        return self
