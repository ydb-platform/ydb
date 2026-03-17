from typing import Any, BinaryIO, Iterator, Optional, Union

from ..core import DatabricksError
from ..service.workspace import (ExportFormat, ImportFormat, Language,
                                 ObjectInfo, ObjectType, WorkspaceAPI)


def _fqcn(x: Any) -> str:
    return f"{x.__module__}.{x.__name__}"


class WorkspaceExt(WorkspaceAPI):
    __doc__ = WorkspaceAPI.__doc__

    def list(
        self,
        path: str,
        *,
        notebooks_modified_after: Optional[int] = None,
        recursive: Optional[bool] = False,
        **kwargs,
    ) -> Iterator[ObjectInfo]:
        """List workspace objects

        :param recursive: bool
            Optionally invoke recursive traversal

        :returns: Iterator of workspaceObjectInfo
        """
        parent_list = super().list
        queue = [path]
        while queue:
            path, queue = queue[0], queue[1:]
            for object_info in parent_list(path, notebooks_modified_after=notebooks_modified_after):
                if recursive and object_info.object_type == ObjectType.DIRECTORY:
                    queue.append(object_info.path)
                    continue
                yield object_info

    def upload(
        self,
        path: str,
        content: Union[bytes, BinaryIO],
        *,
        format: Optional[ImportFormat] = None,
        language: Optional[Language] = None,
        overwrite: Optional[bool] = False,
    ) -> None:
        """
        Uploads a workspace object (for example, a notebook or file) or the contents of an entire
        directory (`DBC` format).

        Errors:
         * `RESOURCE_ALREADY_EXISTS`: if `path` already exists no `overwrite=True`.
         * `INVALID_PARAMETER_VALUE`: if `format` and `content` values are not compatible.

        :param path:     target location of the file on workspace.
        :param content:  the contents as either raw binary data `bytes` or a file-like the file-like `io.BinaryIO` of the `path` contents.
        :param format:   By default, `ImportFormat.SOURCE`. If using `ImportFormat.AUTO` the `path`
                         is imported or exported as either a workspace file or a notebook, depending
                         on an analysis of the `item`’s extension and the header content provided in
                         the request. In addition, if the `path` is imported as a notebook, then
                         the `item`’s extension is automatically removed.
        :param language: Only required if using `ExportFormat.SOURCE`.
        """
        if format is not None and not isinstance(format, ImportFormat):
            raise ValueError(f"format is expected to be {_fqcn(ImportFormat)}, but got {_fqcn(format.__class__)}")
        if (not format or format == ImportFormat.SOURCE) and not language:
            suffixes = {
                ".py": Language.PYTHON,
                ".sql": Language.SQL,
                ".scala": Language.SCALA,
                ".R": Language.R,
            }
            for sfx, lang in suffixes.items():
                if path.endswith(sfx):
                    language = lang
                    break
        if language is not None and not isinstance(language, Language):
            raise ValueError(f"language is expected to be {_fqcn(Language)}, but got {_fqcn(language.__class__)}")
        data = {"path": path}
        if format:
            data["format"] = format.value
        if language:
            data["language"] = language.value
        if overwrite:
            data["overwrite"] = "true"
        try:
            return self._api.do(
                "POST",
                "/api/2.0/workspace/import",
                files={"content": content},
                data=data,
            )
        except DatabricksError as e:
            if e.error_code == "INVALID_PARAMETER_VALUE":
                msg = f"Perhaps you forgot to specify the `format=ImportFormat.AUTO`. {e}"
                raise DatabricksError(message=msg, error_code=e.error_code)
            else:
                raise e

    def download(self, path: str, *, format: Optional[ExportFormat] = None) -> BinaryIO:
        """
        Downloads notebook or file from the workspace

        :param path:     location of the file or notebook on workspace.
        :param format:   By default, `ExportFormat.SOURCE`. If using `ExportFormat.AUTO` the `path`
                         is imported or exported as either a workspace file or a notebook, depending
                         on an analysis of the `item`’s extension and the header content provided in
                         the request.
        :return:         file-like `io.BinaryIO` of the `path` contents.
        """
        query = {"path": path, "direct_download": "true"}
        if format:
            query["format"] = format.value
        response = self._api.do("GET", "/api/2.0/workspace/export", query=query, raw=True)
        return response["contents"]
