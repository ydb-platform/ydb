import os
from typing import IO, TYPE_CHECKING, AnyStr, Callable

from office365.runtime.client_result import ClientResult
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath

if TYPE_CHECKING:
    from office365.sharepoint.files.file import File
    from office365.sharepoint.folders.folder import Folder


class MoveCopyUtil(Entity):
    """A container class for static move/copy methods."""

    @staticmethod
    def copy_file_by_path(context, src_path, dest_path, overwrite, options=None):
        """
        Copies a file from a source URL to a destination URL.

        :param office365.sharepoint.client_context.ClientContext context: client context
        :param str src_path: A full or server relative path that represents the source file.
        :param str dest_path:  A full or server relative url that represents the destination file.
        :param bool overwrite: Overwrites the destination file when it exists.
        :param office365.sharepoint.utilities.move_copy_options.MoveCopyOptions or None options:
        """
        return_type = ClientResult(context)
        payload = {
            "srcPath": SPResPath.create_absolute(context.base_url, src_path),
            "destPath": SPResPath.create_absolute(context.base_url, dest_path),
            "overwrite": overwrite,
            "options": options,
        }
        qry = ServiceOperationQuery(
            MoveCopyUtil(context),
            "CopyFileByPath",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def copy_folder(context, src_url, dest_url, options=None):
        """
        Copies a folder from a source URL to a destination URL.

        :param office365.sharepoint.client_context.ClientContext context: Client context
        :param str src_url: A full or server relative url that represents the source folder.
        :param str dest_url: A full or server relative url that represents the destination folder.
        :param office365.sharepoint.utilities.move_copy_options.MoveCopyOptions options: Contains options used to
            modify the behavior.
        """
        return_type = ClientResult(context)
        binding_type = MoveCopyUtil(context)
        payload = {
            "srcUrl": str(SPResPath.create_absolute(context.base_url, src_url)),
            "destUrl": str(SPResPath.create_absolute(context.base_url, dest_url)),
            "options": options,
        }
        qry = ServiceOperationQuery(
            binding_type, "CopyFolder", None, payload, None, return_type, True
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def copy_folder_by_path(context, src_path, dest_path, options=None):
        """
        Copies a folder from a source URL to a destination URL.

        :param office365.sharepoint.client_context.ClientContext context: client context
        :param str src_path: A full or server relative path that represents the source folder.
        :param str dest_path:  A full or server relative url that represents the destination folder.
        :param office365.sharepoint.utilities.move_copy_options.MoveCopyOptions or None options:
        """
        return_type = ClientResult(context)
        payload = {
            "srcPath": SPResPath.create_absolute(context.base_url, src_path),
            "destPath": SPResPath.create_absolute(context.base_url, dest_path),
            "options": options,
        }
        qry = ServiceOperationQuery(
            MoveCopyUtil(context),
            "CopyFolderByPath",
            None,
            payload,
            None,
            return_type,
            True,
        )
        context.add_query(qry)
        return return_type

    @staticmethod
    def move_folder(context, src_url, dest_url, options):
        """
        Moves a folder from a source URL to a destination URL.

        :param office365.sharepoint.client_context.ClientContext context: client context
        :param str src_url: A full or server relative url that represents the source folder.
        :param str dest_url: A full or server relative url that represents the destination folder.
        :param office365.sharepoint.utilities.move_copy_options.MoveCopyOptions options: Contains options used to
            modify the behavior.
        """
        binding_type = MoveCopyUtil(context)
        payload = {
            "srcUrl": str(SPResPath.create_absolute(context.base_url, src_url)),
            "destUrl": str(SPResPath.create_absolute(context.base_url, dest_url)),
            "options": options,
        }
        qry = ServiceOperationQuery(
            binding_type, "MoveFolder", None, payload, None, None, True
        )
        context.add_query(qry)
        return binding_type

    @staticmethod
    def move_folder_by_path(context, src_path, dest_path, options):
        """
        Moves a folder from a source URL to a destination URL.

        :param str src_path: A full or server relative path that represents the source folder.
        :param str dest_path: A full or server relative path that represents the destination folder.
        :param office365.sharepoint.client_context.ClientContext context: client context
        :param office365.sharepoint.utilities.move_copy_options.MoveCopyOptions options: Contains options used
            to modify the behavior.
        """
        binding_type = MoveCopyUtil(context)
        payload = {
            "srcPath": SPResPath.create_absolute(context.base_url, src_path),
            "destPath": SPResPath.create_absolute(context.base_url, dest_path),
            "options": options,
        }
        qry = ServiceOperationQuery(
            binding_type, "MoveFolderByPath", None, payload, None, None, True
        )
        context.add_query(qry)
        return binding_type

    @staticmethod
    def download_folder(
        remove_folder, download_file, after_file_downloaded=None, recursive=True
    ):
        # type: (Folder, IO, Callable[[File], None], bool) -> Folder
        """
        Downloads a folder into a zip file
        :param office365.sharepoint.folders.folder.Folder remove_folder: Parent folder
        :param typing.IO download_file: A download zip file object
        :param (office365.sharepoint.files.file.File)->None after_file_downloaded: A download callback
        :param bool recursive: Determines whether to traverse folders recursively
        """
        import zipfile

        def _get_relative_file_path(file):
            # type: (File) -> str
            return os.path.join(
                file.parent_folder.serverRelativeUrl.replace(
                    remove_folder.serverRelativeUrl, ""
                ),
                file.name,
            )

        def _download_file(file):
            # type: (File) -> None

            def _after_downloaded(result):
                # type: (ClientResult[AnyStr]) -> None
                filename = _get_relative_file_path(file)
                if callable(after_file_downloaded):
                    after_file_downloaded(file)
                with zipfile.ZipFile(
                    download_file.name, "a", zipfile.ZIP_DEFLATED
                ) as zf:
                    zf.writestr(filename, result.value)

            file.get_content().after_execute(_after_downloaded)

        def _download_folder(folder):
            # type: (Folder) -> None

            def _download_files(rt):
                [_download_file(file) for file in folder.files]
                if recursive:
                    [_download_folder(sub_folder) for sub_folder in folder.folders]

            folder.expand(["Files", "Folders"]).get().after_execute(_download_files)

        _download_folder(remove_folder)
        return remove_folder
