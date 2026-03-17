from office365.runtime.paths.service_operation import ServiceOperationPath
from office365.sharepoint.contentcenter.machinelearning.models.model import (
    SPMachineLearningModel,
)
from office365.sharepoint.entity_collection import EntityCollection


class SPMachineLearningModelCollection(EntityCollection[SPMachineLearningModel]):
    def __init__(self, context, resource_path=None):
        super(SPMachineLearningModelCollection, self).__init__(
            context, SPMachineLearningModel, resource_path
        )

    def get_by_title(self, title):
        """
        :param str title: The title of the model to return.
        """
        return SPMachineLearningModel(
            self.context,
            ServiceOperationPath("GetByTitle", [title], self.resource_path),
        )
