import os

from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.runtime.paths.v3.entity import EntityPath
from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity_collection import EntityCollection
from office365.sharepoint.folders.coloring_information import FolderColoringInformation
from office365.sharepoint.folders.folder import Folder
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath


class FolderCollection(EntityCollection[Folder]):
    """Represents a collection of Folder resources."""

    def __init__(self, context, resource_path=None, parent=None):
        super(FolderCollection, self).__init__(context, Folder, resource_path, parent)

    def add_using_path(self, decoded_url, overwrite):
        """
        Adds the folder located at the specified path to the collection.
        :param str decoded_url: Specifies the path for the folder.
        :param bool overwrite:  bool
        """
        parameters = {"DecodedUrl": decoded_url, "Overwrite": overwrite}
        return_type = Folder(self.context)
        qry = ServiceOperationQuery(
            self, "AddUsingPath", parameters, None, None, return_type
        )
        self.context.add_query(qry)
        return return_type

    def ensure_path(self, path):
        """
        Ensures a folder exist
        :param str path: server or site relative url to a folder
        """
        path = path.replace(self.context.site_path, "")
        names = [name for name in path.split("/") if name]
        if not names:
            raise ValueError("Invalid server or site relative url")

        name, child_names = names[0], names[1:]
        folder = self.add(name)
        for name in child_names:
            folder = folder.add(name)
        return folder

    def add(self, name, color_hex=None):
        """Adds the folder that is located at the specified URL to the collection.
        :param str name: Specifies the Name or Path of the folder.
        :param str color_hex: Specifies the color of the folder.
        """
        return_type = Folder(self.context, EntityPath(name, self.resource_path))
        if color_hex:

            def _add_coloring():
                path = os.path.join(
                    self.parent.properties.get("ServerRelativeUrl"), name
                )
                coloring_info = FolderColoringInformation(color_hex=color_hex)
                self.context.folder_coloring.create_folder(
                    path, coloring_info, return_type=return_type
                )

            self.parent.ensure_property("ServerRelativeUrl", _add_coloring)
        else:
            self.add_child(return_type)
            qry = ServiceOperationQuery(self, "Add", [name], None, None, return_type)
            self.context.add_query(qry)
        return return_type

    def get_by_url(self, url):
        """Retrieve Folder resource by url
        :param str url: Specifies the URL of the list folder. The URL MUST be an absolute URL, a server-relative URL,
            a site-relative URL relative to the site (2) containing the collection of list folders, or relative to the
            list folder that directly contains this collection of list folders.
        """
        return Folder(
            self.context, ServiceOperationPath("GetByUrl", [url], self.resource_path)
        )

    def get_by_path(self, decoded_url):
        """
        Get folder at the specified path.
        :param str decoded_url: Specifies the path for the folder.
        """
        return Folder(
            self.context,
            ServiceOperationPath(
                "GetByPath", SPResPath(decoded_url), self.resource_path
            ),
        )
