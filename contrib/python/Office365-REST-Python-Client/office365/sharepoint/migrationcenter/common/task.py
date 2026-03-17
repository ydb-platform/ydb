from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.migrationcenter.common.task_entity_data import (
    MigrationTaskEntityData,
)


class MigrationTask(MigrationTaskEntityData):

    def __init__(self, context):
        static_path = ResourcePath(
            "Microsoft.Online.SharePoint.MigrationCenter.Service.MigrationTask"
        )
        super(MigrationTask, self).__init__(context, static_path)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MigrationCenter.Service.MigrationTask"
