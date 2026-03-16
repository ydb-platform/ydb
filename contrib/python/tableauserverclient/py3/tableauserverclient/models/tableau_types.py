from typing import Union

from tableauserverclient.models.database_item import DatabaseItem
from tableauserverclient.models.datasource_item import DatasourceItem
from tableauserverclient.models.flow_item import FlowItem
from tableauserverclient.models.project_item import ProjectItem
from tableauserverclient.models.table_item import TableItem
from tableauserverclient.models.view_item import ViewItem
from tableauserverclient.models.workbook_item import WorkbookItem
from tableauserverclient.models.metric_item import MetricItem
from tableauserverclient.models.virtual_connection_item import VirtualConnectionItem


class Resource:
    Database = "database"
    Datarole = "datarole"
    Table = "table"
    Datasource = "datasource"
    Flow = "flow"
    Lens = "lens"
    Metric = "metric"
    Project = "project"
    View = "view"
    VirtualConnection = "virtualConnection"
    Workbook = "workbook"


# resource types that have permissions, can be renamed, etc
# todo: refactoring: should actually define TableauItem as an interface and let all these implement it
TableauItem = Union[
    DatasourceItem,
    FlowItem,
    MetricItem,
    ProjectItem,
    ViewItem,
    WorkbookItem,
    VirtualConnectionItem,
    DatabaseItem,
    TableItem,
]


def plural_type(content_type: Union[Resource, str]) -> str:
    if content_type == Resource.Lens:
        return "lenses"
    else:
        return f"{content_type}s"
