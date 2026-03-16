from office365.sharepoint.contentcenter.machinelearning.workitems.item import (
    SPMachineLearningWorkItem,
)
from office365.sharepoint.entity_collection import EntityCollection


class SPMachineLearningWorkItemCollection(EntityCollection):
    def __init__(self, context, resource_path=None):
        super(SPMachineLearningWorkItemCollection, self).__init__(
            context, SPMachineLearningWorkItem, resource_path
        )
