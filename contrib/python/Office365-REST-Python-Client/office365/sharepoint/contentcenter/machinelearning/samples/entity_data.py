from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity


class SPMachineLearningSampleEntityData(Entity):
    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.Office.Server.ContentCenter.SPMachineLearningSampleEntityData"
        )
        super(SPMachineLearningSampleEntityData, self).__init__(context, static_path)

    @property
    def entity_type_name(self):
        return "Microsoft.Office.Server.ContentCenter.SPMachineLearningSampleEntityData"
