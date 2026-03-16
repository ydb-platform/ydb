import xml.etree.ElementTree as ET
import logging
from .workbook_item import WorkbookItem
from .view_item import ViewItem
from .project_item import ProjectItem
from .datasource_item import DatasourceItem

logger = logging.getLogger('tableau.models.favorites_item')


class FavoriteItem:
    class Type:
        Workbook = 'workbook'
        Datasource = 'datasource'
        View = 'view'
        Project = 'project'

    @classmethod
    def from_response(cls, xml, namespace):
        favorites = {
            'datasources': [],
            'projects':    [],
            'views':       [],
            'workbooks':   [],
        }

        parsed_response = ET.fromstring(xml)
        for workbook in parsed_response.findall('.//t:favorite/t:workbook', namespace):
            fav_workbook = WorkbookItem('')
            fav_workbook._set_values(*fav_workbook._parse_element(workbook, namespace))
            if fav_workbook:
                favorites['workbooks'].append(fav_workbook)
        for view in parsed_response.findall('.//t:favorite[t:view]', namespace):
            fav_views = ViewItem.from_xml_element(view, namespace)
            if fav_views:
                for fav_view in fav_views:
                    favorites['views'].append(fav_view)
        for datasource in parsed_response.findall('.//t:favorite/t:datasource', namespace):
            fav_datasource = DatasourceItem('')
            fav_datasource._set_values(*fav_datasource._parse_element(datasource, namespace))
            if fav_datasource:
                favorites['datasources'].append(fav_datasource)
        for project in parsed_response.findall('.//t:favorite/t:project', namespace):
            fav_project = ProjectItem('p')
            fav_project._set_values(*fav_project._parse_element(project))
            if fav_project:
                favorites['projects'].append(fav_project)

        return favorites
