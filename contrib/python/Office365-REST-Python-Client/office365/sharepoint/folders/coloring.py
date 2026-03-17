from typing_extensions import Self

from office365.runtime.queries.service_operation import ServiceOperationQuery
from office365.sharepoint.entity import Entity
from office365.sharepoint.folders.coloring_information import FolderColoringInformation
from office365.sharepoint.folders.folder import Folder
from office365.sharepoint.types.resource_path import ResourcePath as SPResPath


class FolderColoring(Entity):
    """"""

    def create_folder(
        self,
        decoded_url,
        coloring_information=FolderColoringInformation(color_hex="1"),
        return_type=None,
    ):
        """
        :param str decoded_url:
        :param FolderColoringInformation coloring_information:
        :param Folder return_type: Return type
        """
        if return_type is None:
            return_type = Folder(self.context)

        payload = {
            "path": SPResPath(decoded_url),
            "coloringInformation": coloring_information,
        }
        qry = ServiceOperationQuery(
            self, "CreateFolder", parameters_type=payload, return_type=return_type
        )
        self.context.add_query(qry)
        return return_type

    def stamp_color(self, decoded_url, coloring_information):
        # type: (str, FolderColoringInformation) -> Self
        """
        :param str decoded_url:
        :param FolderColoringInformation coloring_information:
        """
        payload = {
            "DecodedUrl": decoded_url,
            "coloringInformation": coloring_information,
        }
        qry = ServiceOperationQuery(self, "StampColor", parameters_type=payload)
        self.context.add_query(qry)
        return self
