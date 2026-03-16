import json
from typing import IO, AnyStr, Callable

from typing_extensions import Self

from office365.runtime.client_result import ClientResult
from office365.sharepoint.files.system_object_type import FileSystemObjectType
from office365.sharepoint.listitems.collection import ListItemCollection
from office365.sharepoint.listitems.listitem import ListItem
from office365.sharepoint.lists.list import List


class ListExporter(object):
    """ """

    @staticmethod
    def export(
        source_list, destination_file, include_content=False, item_exported=None
    ):
        # type: (List, IO, bool, Callable[[ListItem], None]) -> Self
        """Exports SharePoint List"""
        import zipfile

        def _append_file(name, data):
            with zipfile.ZipFile(
                destination_file.name, "a", zipfile.ZIP_DEFLATED
            ) as zf:
                zf.writestr(name, data)

        def _download_content(list_item):
            # type: (ListItem) -> None
            def _after_downloaded(result):
                # type: (ClientResult[AnyStr]) -> None
                item_path = list_item.properties["FileRef"].replace(
                    source_list.root_folder.serverRelativeUrl, ""
                )
                _append_file(item_path, result.value)

            list_item.file.get_content().after_execute(_after_downloaded)

        def _export_items(items):
            # type: (ListItemCollection) -> None

            for item in items:
                item_path = str(item.id) + ".json"

                if item.file_system_object_type == FileSystemObjectType.File:
                    _append_file(item_path, json.dumps(item.to_json()))

                    if include_content:
                        _download_content(item)

                    if callable(item_exported):
                        item_exported(item)

        def _get_items():
            (
                source_list.items.select(
                    [
                        "*",
                        "Id",
                        "FileRef",
                        "FileDirRef",
                        "FileLeafRef",
                        "FileSystemObjectType",
                    ]
                )
                .get()
                .paged(page_loaded=_export_items)
            )

        source_list.ensure_properties(["SchemaXml", "RootFolder"], _get_items)

        return source_list
