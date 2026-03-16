from office365.sharepoint.entity import Entity


class SPMachineLearningWorkItem(Entity):
    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.ContentCenter.SPMachineLearningWorkItem"
