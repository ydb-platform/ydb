from office365.entity_collection import EntityCollection
from office365.onedrive.workbooks.charts.chart import WorkbookChart


class WorkbookChartCollection(EntityCollection[WorkbookChart]):
    def __init__(self, context, resource_path=None):
        super(WorkbookChartCollection, self).__init__(
            context, WorkbookChart, resource_path
        )
