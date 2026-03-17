import hashlib
import os
import uuid
from typing import IO, TYPE_CHECKING, Callable

from office365.runtime.client_result import ClientResult
from office365.runtime.paths.resource_path import ResourcePath
from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.files.creation_information import FileCreationInformation
from office365.sharepoint.files.file import File
from office365.sharepoint.files.publish.status import FileStatus
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath

if TYPE_CHECKING:
    from office365.sharepoint.folders.folder import Folder


class FileCollection(EntityCollection[File]):
    """Represents a collection of File resources."""

    def __init__(self, context, resource_path=None, parent=None):
        # type: (ClientContext, ResourcePath, Folder) -> None
        super(FileCollection, self).__init__(context, File, resource_path, parent)

    def get_published_file(self, base_file_path):
        """ """
        return_type = FileStatus(self.context)
        qry = ServiceOperationQuery(
            self, "GetPublishedFile", [base_file_path], None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def upload(self, path_or_file, file_name=None):
        # type: (str|IO, str) -> File
        """Uploads a file into folder.

        Note: This method only supports files up to 4MB in size!
        Consider create_upload_session method instead for larger files
        :param str or typing.IO path_or_file: Path where file to upload resides or file handle
        :param str file_name: New file name
        """
        if hasattr(path_or_file, "read"):
            name = file_name or os.path.basename(path_or_file.name)
            content = path_or_file.read()
            return self.add(name, content, True)
        else:
            with open(path_or_file, "rb") as f:
                content = f.read()
            name = file_name or os.path.basename(path_or_file)
            return self.add(name, content, True)

    def upload_with_checksum(self, file_object, chunk_size=1024):
        # type: (IO, int) -> File
        """ """
        h = hashlib.md5()
        file_name = os.path.basename(file_object.name)
        upload_id = str(uuid.uuid4())

        def _upload_session(return_type):
            # type: (File) -> None
            content = file_object.read(chunk_size)
            h.update(content)

            return_type.upload_with_checksum(
                upload_id, h.hexdigest(), content
            )  # .after_execute(_upload_session)

        return self.add(file_name, None, True).after_execute(_upload_session)

    def create_upload_session(
        self, file, chunk_size, chunk_uploaded=None, file_name=None, **kwargs
    ):
        # type: (IO|str, int, Callable[[int, ...], None], str, ...) -> File
        """Upload a file as multiple chunks
        :param str or typing.IO file: path where file to upload resides or file handle
        :param int chunk_size: upload chunk size (in bytes)
        :param (long)->None or None chunk_uploaded: uploaded event
        :param str file_name: custom file name
        :param kwargs: arguments to pass to chunk_uploaded function
        """

        auto_close = False
        if not hasattr(file, "read"):
            file = open(file, "rb")
            auto_close = True

        file_size = os.fstat(file.fileno()).st_size
        file_name = file_name if file_name else os.path.basename(file.name)
        upload_id = str(uuid.uuid4())

        def _upload(return_type):
            # type: (File) -> None

            def _after_uploaded(result):
                # type: (ClientResult) -> None
                _upload(return_type)

            uploaded_bytes = file.tell()
            if callable(chunk_uploaded):
                chunk_uploaded(uploaded_bytes, **kwargs)

            content = file.read(chunk_size)
            if uploaded_bytes == file_size:
                if auto_close and not file.closed:
                    file.close()
                return

            if uploaded_bytes == 0:
                return_type.start_upload(upload_id, content).after_execute(
                    _after_uploaded
                )
            elif uploaded_bytes + len(content) < file_size:
                return_type.continue_upload(
                    upload_id, uploaded_bytes, content
                ).after_execute(_after_uploaded)
            else:
                return_type.finish_upload(
                    upload_id, uploaded_bytes, content
                ).after_execute(_upload)

        if file_size > chunk_size:
            return self.add(file_name, None, True).after_execute(_upload)
        else:
            return self.add(file_name, file.read(), True)

    def add(self, url, content, overwrite=False):
        """
        Adds a file to the collection based on provided file creation information. A reference to the SP.File that
        was added is returned.

        :param str url: Specifies the URL of the file to be added. It MUST NOT be NULL. It MUST be a URL of relative
            or absolute form. Its length MUST be equal to or greater than 1.
        :param bool overwrite: Specifies whether to overwrite an existing file with the same name and in the same
            location as the one being added.
        :param str or bytes or None content: Specifies the binary content of the file to be added.
        """
        return_type = File(self.context)
        self.add_child(return_type)
        params = FileCreationInformation(url=url, overwrite=overwrite)
        qry = ServiceOperationQuery(
            self, "add", params.to_json(), content, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def add_template_file(self, url_of_file, template_file_type):
        """Adds a ghosted file to an existing list or document library.

        :param int template_file_type: refer TemplateFileType enum
        :param str url_of_file: server relative url of a file
        """
        return_type = File(self.context)
        self.add_child(return_type)

        def _add_template_file():
            params = {
                "urlOfFile": str(
                    SPResPath.create_relative(
                        self.parent.properties["ServerRelativeUrl"], url_of_file
                    )
                ),
                "templateFileType": template_file_type,
            }
            qry = ServiceOperationQuery(
                self, "addTemplateFile", params, None, None, return_type
            )
            self.context.add_query(qry)

        self.parent.ensure_property("ServerRelativeUrl", _add_template_file)
        return return_type

    def get_by_url(self, url):
        # type: (str) -> File
        """Retrieve File object by url"""
        return File(
            self.context, ServiceOperationPath("GetByUrl", [url], self.resource_path)
        )

    def get_by_id(self, _id):
        # type: (int) -> File
        """Gets the File with the specified ID."""
        return File(
            self.context, ServiceOperationPath("getById", [_id], self.resource_path)
        )

    @property
    def parent(self):
        # type: () -> Folder
        return self._parent
