from office365.runtime.paths.resource_path import ResourcePath
from office365.sharepoint.entity import Entity
from office365.sharepoint.migrationcenter.service.performance.data import (
    PerformanceDataCollection,
)
from office365.sharepoint.migrationcenter.service.teams import MigrationCenterTeams


class MigrationCenterServices(Entity):
    def __init__(self, context, resource_path=None):
        if resource_path is None:
            resource_path = ResourcePath(
                "Microsoft.Online.SharePoint.MigrationCenter.Service.MigrationCenterServices"
            )
        super(MigrationCenterServices, self).__init__(context, resource_path)

    @property
    def performance_data(self):
        # type: () -> PerformanceDataCollection
        """"""
        return self.properties.get(
            "PerformanceData",
            PerformanceDataCollection(
                self.context, ResourcePath("PerformanceData", self.resource_path)
            ),
        )

    @property
    def teams(self):
        # type: () -> PerformanceDataCollection
        """"""
        return self.properties.get(
            "Teams",
            MigrationCenterTeams(
                self.context, ResourcePath("Teams", self.resource_path)
            ),
        )

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MigrationCenter.Service.MigrationCenterServices"
