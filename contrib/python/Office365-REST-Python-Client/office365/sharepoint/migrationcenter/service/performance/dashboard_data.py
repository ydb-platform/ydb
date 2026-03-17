from office365.runtime.client_value import ClientValue
from office365.runtime.client_value_collection import ClientValueCollection
from office365.runtime.types.collections import StringCollection
from office365.sharepoint.migrationcenter.service.performance.throughput_data import (
    ThroughputData,
)


class PerformanceDashboardData(ClientValue):
    """"""

    def __init__(
        self, bottleneck_list=None, recommendation_list=None, throughput_trend=None
    ):
        self.BottleneckList = StringCollection(bottleneck_list)
        self.RecommendationList = StringCollection(recommendation_list)
        self.ThroughputTrend = ClientValueCollection(ThroughputData, throughput_trend)

    @property
    def entity_type_name(self):
        return "Microsoft.Online.SharePoint.MigrationCenter.Service.PerformanceDashboardData"
