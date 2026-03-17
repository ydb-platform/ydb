import xml.etree.ElementTree as ET
from typing import Optional, overload

from defusedxml.ElementTree import fromstring

from tableauserverclient.models.exceptions import UnpopulatedPropertyError
from tableauserverclient.models.property_decorators import property_is_enum
from tableauserverclient.models.user_item import UserItem


class ProjectItem:
    """
    The project resources for Tableau are defined in the ProjectItem class. The
    class corresponds to the project resources you can access using the Tableau
    Server REST API.

    Parameters
    ----------
    name : str
        Name of the project.

    description : str
        Description of the project.

    content_permissions : str
        Sets or shows the permissions for the content in the project. The
        options are either LockedToProject, ManagedByOwner or
        LockedToProjectWithoutNested.

    parent_id : str
        The id of the parent project. Use this option to create project
        hierarchies. For information about managing projects, project
        hierarchies, and permissions, see
        https://help.tableau.com/current/server/en-us/projects.htm

    samples : bool
        Set to True to include sample workbooks and data sources in the
        project. The default is False.

    Attributes
    ----------
    datasource_count : int
        The number of data sources in the project.

    id : str
        The unique identifier for the project.

    owner: Optional[UserItem]
        The UserItem owner of the project.

    owner_id : str
        The unique identifier for the UserItem owner of the project.

    project_count : int
        The number of projects in the project.

    top_level_project : bool
        True if the project is a top-level project.

    view_count : int
        The number of views in the project.

    workbook_count : int
        The number of workbooks in the project.

    writeable : bool
        True if the project is writeable.
    """

    ERROR_MSG = "Project item must be populated with permissions first."

    class ContentPermissions:
        LockedToProject: str = "LockedToProject"
        ManagedByOwner: str = "ManagedByOwner"
        LockedToProjectWithoutNested: str = "LockedToProjectWithoutNested"

    def __repr__(self):
        return "<Project {} {} parent={} permissions={}>".format(
            self._id, self.name, self.parent_id or "None (Top level)", self.content_permissions or "Not Set"
        )

    def __init__(
        self,
        name: Optional[str] = None,
        description: Optional[str] = None,
        content_permissions: Optional[str] = None,
        parent_id: Optional[str] = None,
        samples: Optional[bool] = None,
        id: Optional[str] = None,
    ) -> None:
        self._content_permissions = None
        self._id: Optional[str] = id
        self.description: Optional[str] = description
        self.name: str = name
        self.content_permissions: Optional[str] = content_permissions
        self.parent_id: Optional[str] = parent_id
        self._samples: Optional[bool] = samples
        self._owner_id: Optional[str] = None
        self._top_level_project: Optional[bool] = None
        self._writeable: Optional[bool] = None

        self._permissions = None
        self._default_workbook_permissions = None
        self._default_datasource_permissions = None
        self._default_flow_permissions = None
        self._default_lens_permissions = None
        self._default_datarole_permissions = None
        self._default_metric_permissions = None
        self._default_virtualconnection_permissions = None
        self._default_database_permissions = None
        self._default_table_permissions = None

        self._project_count: Optional[int] = None
        self._workbook_count: Optional[int] = None
        self._view_count: Optional[int] = None
        self._datasource_count: Optional[int] = None

        self._owner: Optional[UserItem] = None

    @property
    def content_permissions(self):
        return self._content_permissions

    @content_permissions.setter
    @property_is_enum(ContentPermissions)
    def content_permissions(self, value: Optional[str]) -> None:
        self._content_permissions = value

    @property
    def permissions(self):
        if self._permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._permissions()

    @property
    def default_datasource_permissions(self):
        if self._default_datasource_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_datasource_permissions()

    @property
    def default_workbook_permissions(self):
        if self._default_workbook_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_workbook_permissions()

    @property
    def default_flow_permissions(self):
        if self._default_flow_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_flow_permissions()

    @property
    def default_lens_permissions(self):
        if self._default_lens_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_lens_permissions()

    @property
    def default_datarole_permissions(self):
        if self._default_datarole_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_datarole_permissions()

    @property
    def default_metric_permissions(self):
        if self._default_metric_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_metric_permissions()

    @property
    def default_virtualconnection_permissions(self):
        if self._default_virtualconnection_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_virtualconnection_permissions()

    @property
    def default_database_permissions(self):
        if self._default_database_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_database_permissions()

    @property
    def default_table_permissions(self):
        if self._default_table_permissions is None:
            raise UnpopulatedPropertyError(self.ERROR_MSG)
        return self._default_table_permissions()

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        self._name = value

    @property
    def owner_id(self) -> Optional[str]:
        return self._owner_id

    @owner_id.setter
    def owner_id(self, value: str) -> None:
        self._owner_id = value

    @property
    def top_level_project(self) -> Optional[bool]:
        return self._top_level_project

    @property
    def writeable(self) -> Optional[bool]:
        return self._writeable

    @property
    def project_count(self) -> Optional[int]:
        return self._project_count

    @property
    def workbook_count(self) -> Optional[int]:
        return self._workbook_count

    @property
    def view_count(self) -> Optional[int]:
        return self._view_count

    @property
    def datasource_count(self) -> Optional[int]:
        return self._datasource_count

    @property
    def owner(self) -> Optional[UserItem]:
        return self._owner

    def is_default(self):
        return self.name.lower() == "default"

    def _set_values(
        self,
        project_id,
        name,
        description,
        content_permissions,
        parent_id,
        owner_id,
        top_level_project,
        writeable,
        project_count,
        workbook_count,
        view_count,
        datasource_count,
        owner,
    ):
        if project_id is not None:
            self._id = project_id
        if name:
            self._name = name
        if description:
            self.description = description
        if content_permissions:
            self._content_permissions = content_permissions
        if parent_id:
            self.parent_id = parent_id
        if owner_id:
            self._owner_id = owner_id
        if project_count is not None:
            self._project_count = project_count
        if workbook_count is not None:
            self._workbook_count = workbook_count
        if view_count is not None:
            self._view_count = view_count
        if datasource_count is not None:
            self._datasource_count = datasource_count
        if top_level_project is not None:
            self._top_level_project = top_level_project
        if writeable is not None:
            self._writeable = writeable
        if owner is not None:
            self._owner = owner

    def _set_permissions(self, permissions):
        self._permissions = permissions

    def _set_default_permissions(self, permissions, content_type):
        attr = f"_default_{content_type}_permissions".lower()
        setattr(
            self,
            attr,
            permissions,
        )

    @classmethod
    def from_response(cls, resp: bytes, ns: Optional[dict]) -> list["ProjectItem"]:
        all_project_items = list()
        parsed_response = fromstring(resp)
        all_project_xml = parsed_response.findall(".//t:project", namespaces=ns)

        for project_xml in all_project_xml:
            project_item = cls.from_xml(project_xml, namespace=ns)
            all_project_items.append(project_item)
        return all_project_items

    @classmethod
    def from_xml(cls, project_xml: ET.Element, namespace: Optional[dict] = None) -> "ProjectItem":
        project_item = cls()
        project_item._set_values(*cls._parse_element(project_xml, namespace))
        return project_item

    @staticmethod
    def _parse_element(project_xml: ET.Element, namespace: Optional[dict]) -> tuple:
        id = project_xml.get("id", None)
        name = project_xml.get("name", None)
        description = project_xml.get("description", None)
        content_permissions = project_xml.get("contentPermissions", None)
        parent_id = project_xml.get("parentProjectId", None)
        top_level_project = str_to_bool(project_xml.get("topLevelProject", None))
        writeable = str_to_bool(project_xml.get("writeable", None))
        owner_id = None
        owner = None
        if (owner_elem := project_xml.find(".//t:owner", namespaces=namespace)) is not None:
            owner = UserItem.from_xml(owner_elem, namespace)
            owner_id = owner_elem.get("id", None)

        project_count = None
        workbook_count = None
        view_count = None
        datasource_count = None
        if (count_elem := project_xml.find(".//t:contentsCounts", namespaces=namespace)) is not None:
            project_count = int(count_elem.get("projectCount", 0))
            workbook_count = int(count_elem.get("workbookCount", 0))
            view_count = int(count_elem.get("viewCount", 0))
            datasource_count = int(count_elem.get("dataSourceCount", 0))

        return (
            id,
            name,
            description,
            content_permissions,
            parent_id,
            owner_id,
            top_level_project,
            writeable,
            project_count,
            workbook_count,
            view_count,
            datasource_count,
            owner,
        )


@overload
def str_to_bool(value: str) -> bool: ...


@overload
def str_to_bool(value: None) -> None: ...


def str_to_bool(value):
    return value.lower() == "true" if value is not None else None
