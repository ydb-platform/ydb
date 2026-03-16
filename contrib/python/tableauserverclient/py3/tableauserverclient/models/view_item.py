import copy
from datetime import datetime
from requests import Response
from typing import TYPE_CHECKING, Callable, Optional, overload
from collections.abc import Iterator

from defusedxml.ElementTree import fromstring

from tableauserverclient.datetime_helpers import parse_datetime
from tableauserverclient.models.exceptions import UnpopulatedPropertyError
from tableauserverclient.models.location_item import LocationItem
from tableauserverclient.models.permissions_item import PermissionsRule
from tableauserverclient.models.project_item import ProjectItem
from tableauserverclient.models.tag_item import TagItem
from tableauserverclient.models.user_item import UserItem

if TYPE_CHECKING:
    from tableauserverclient.models.workbook_item import WorkbookItem


class ViewItem:
    """
    Contains the members or attributes for the view resources on Tableau Server.
    The ViewItem class defines the information you can request or query from
    Tableau Server. The class members correspond to the attributes of a server
    request or response payload.

    Attributes
    ----------
    content_url: Optional[str], default None
        The name of the view as it would appear in a URL.

    created_at: Optional[datetime], default None
        The date and time when the view was created.

    id: Optional[str], default None
        The unique identifier for the view.

    image: Optional[Callable[[], bytes]], default None
        The image of the view. You must first call the `views.populate_image`
        method to access the image.

    location: Optional[LocationItem], default None
        The location of the view. The location can be a personal space or a
        project.

    name: Optional[str], default None
        The name of the view.

    owner: Optional[UserItem], default None
        The owner of the view.

    owner_id: Optional[str], default None
        The ID for the owner of the view.

    pdf: Optional[Callable[[], bytes]], default None
        The PDF of the view. You must first call the `views.populate_pdf`
        method to access the PDF.

    preview_image: Optional[Callable[[], bytes]], default None
        The preview image of the view. You must first call the
        `views.populate_preview_image` method to access the preview image.

    project: Optional[ProjectItem], default None
        The project that contains the view.

    project_id: Optional[str], default None
        The ID for the project that contains the view.

    tags: set[str], default set()
        The tags associated with the view.

    total_views: Optional[int], default None
        The total number of views for the view.

    updated_at: Optional[datetime], default None
        The date and time when the view was last updated.

    workbook: Optional[WorkbookItem], default None
        The workbook that contains the view.

    workbook_id: Optional[str], default None
        The ID for the workbook that contains the view.
    """

    def __init__(self) -> None:
        self._content_url: Optional[str] = None
        self._created_at: Optional[datetime] = None
        self._id: Optional[str] = None
        self._image: Optional[Callable[[], bytes]] = None
        self._initial_tags: set[str] = set()
        self._name: Optional[str] = None
        self._owner_id: Optional[str] = None
        self._preview_image: Optional[Callable[[], bytes]] = None
        self._project_id: Optional[str] = None
        self._pdf: Optional[Callable[[], bytes]] = None
        self._csv: Optional[Callable[[], Iterator[bytes]]] = None
        self._excel: Optional[Callable[[], Iterator[bytes]]] = None
        self._total_views: Optional[int] = None
        self._sheet_type: Optional[str] = None
        self._updated_at: Optional[datetime] = None
        self._workbook_id: Optional[str] = None
        self._permissions: Optional[Callable[[], list[PermissionsRule]]] = None
        self.tags: set[str] = set()
        self._favorites_total: Optional[int] = None
        self._view_url_name: Optional[str] = None
        self._data_acceleration_config = {
            "acceleration_enabled": None,
            "acceleration_status": None,
        }

        self._owner: Optional[UserItem] = None
        self._project: Optional[ProjectItem] = None
        self._workbook: Optional["WorkbookItem"] = None
        self._location: Optional[LocationItem] = None

    def __str__(self):
        return "<ViewItem {} '{}' contentUrl='{}' project={}>".format(
            self._id, self.name, self.content_url, self.project_id
        )

    def __repr__(self):
        return self.__str__() + "  { " + ", ".join(" % s: % s" % item for item in vars(self).items()) + "}"

    def _set_preview_image(self, preview_image):
        self._preview_image = preview_image

    def _set_image(self, image):
        self._image = image

    def _set_pdf(self, pdf):
        self._pdf = pdf

    def _set_csv(self, csv):
        self._csv = csv

    def _set_excel(self, excel):
        self._excel = excel

    @property
    def content_url(self) -> Optional[str]:
        return self._content_url

    @property
    def created_at(self) -> Optional[datetime]:
        return self._created_at

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def image(self) -> bytes:
        if self._image is None:
            error = "View item must be populated with its png image first."
            raise UnpopulatedPropertyError(error)
        return self._image()

    @property
    def name(self) -> Optional[str]:
        return self._name

    @property
    def owner_id(self) -> Optional[str]:
        return self._owner_id

    @property
    def preview_image(self) -> bytes:
        if self._preview_image is None:
            error = "View item must be populated with its preview image first."
            raise UnpopulatedPropertyError(error)
        return self._preview_image()

    @property
    def project_id(self) -> Optional[str]:
        return self._project_id

    @property
    def pdf(self) -> bytes:
        if self._pdf is None:
            error = "View item must be populated with its pdf first."
            raise UnpopulatedPropertyError(error)
        return self._pdf()

    @property
    def csv(self) -> Iterator[bytes]:
        if self._csv is None:
            error = "View item must be populated with its csv first."
            raise UnpopulatedPropertyError(error)
        return self._csv()

    @property
    def excel(self) -> Iterator[bytes]:
        if self._excel is None:
            error = "View item must be populated with its excel first."
            raise UnpopulatedPropertyError(error)
        return self._excel()

    @property
    def sheet_type(self) -> Optional[str]:
        return self._sheet_type

    @property
    def total_views(self):
        if self._total_views is None:
            error = "Usage statistics must be requested when querying for view."
            raise UnpopulatedPropertyError(error)
        return self._total_views

    @property
    def updated_at(self) -> Optional[datetime]:
        return self._updated_at

    @property
    def workbook_id(self) -> Optional[str]:
        return self._workbook_id

    @property
    def view_url_name(self) -> Optional[str]:
        return self._view_url_name

    @property
    def favorites_total(self) -> Optional[int]:
        return self._favorites_total

    @property
    def data_acceleration_config(self):
        return self._data_acceleration_config

    @data_acceleration_config.setter
    def data_acceleration_config(self, value):
        self._data_acceleration_config = value

    @property
    def project(self) -> Optional["ProjectItem"]:
        return self._project

    @property
    def workbook(self) -> Optional["WorkbookItem"]:
        return self._workbook

    @property
    def owner(self) -> Optional[UserItem]:
        return self._owner

    @property
    def location(self) -> Optional[LocationItem]:
        return self._location

    @property
    def permissions(self) -> list[PermissionsRule]:
        if self._permissions is None:
            error = "View item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._permissions()

    def _set_permissions(self, permissions: Callable[[], list[PermissionsRule]]) -> None:
        self._permissions = permissions

    @classmethod
    def from_response(cls, resp: "Response", ns, workbook_id="") -> list["ViewItem"]:
        return cls.from_xml_element(fromstring(resp), ns, workbook_id)

    @classmethod
    def from_xml_element(cls, parsed_response, ns, workbook_id="") -> list["ViewItem"]:
        all_view_items = list()
        all_view_xml = parsed_response.findall(".//t:view", namespaces=ns)
        for view_xml in all_view_xml:
            view_item = cls.from_xml(view_xml, ns, workbook_id)
            all_view_items.append(view_item)
        return all_view_items

    @classmethod
    def from_xml(cls, view_xml, ns, workbook_id="") -> "ViewItem":
        view_item = cls()
        usage_elem = view_xml.find(".//t:usage", namespaces=ns)
        workbook_elem = view_xml.find(".//t:workbook", namespaces=ns)
        owner_elem = view_xml.find(".//t:owner", namespaces=ns)
        project_elem = view_xml.find(".//t:project", namespaces=ns)
        tags_elem = view_xml.find("./t:tags", namespaces=ns)
        data_acceleration_config_elem = view_xml.find(".//t:dataAccelerationConfig", namespaces=ns)
        view_item._created_at = parse_datetime(view_xml.get("createdAt", None))
        view_item._updated_at = parse_datetime(view_xml.get("updatedAt", None))
        view_item._id = view_xml.get("id", None)
        view_item._name = view_xml.get("name", None)
        view_item._content_url = view_xml.get("contentUrl", None)
        view_item._sheet_type = view_xml.get("sheetType", None)
        view_item._favorites_total = string_to_int(view_xml.get("favoritesTotal", None))
        view_item._view_url_name = view_xml.get("viewUrlName", None)
        if usage_elem is not None:
            total_view = usage_elem.get("totalViewCount", None)
            if total_view:
                view_item._total_views = int(total_view)
        if owner_elem is not None:
            user = UserItem.from_xml(owner_elem, ns)
            view_item._owner = user
            view_item._owner_id = owner_elem.get("id", None)
        if project_elem is not None:
            project_item = ProjectItem.from_xml(project_elem, ns)
            view_item._project = project_item
            view_item._project_id = project_item.id
        if workbook_id:
            view_item._workbook_id = workbook_id
        elif workbook_elem is not None:
            from tableauserverclient.models.workbook_item import WorkbookItem

            workbook_item = WorkbookItem.from_xml(workbook_elem, ns)
            view_item._workbook = workbook_item
            view_item._workbook_id = workbook_item.id
        if tags_elem is not None:
            tags = TagItem.from_xml_element(tags_elem, ns)
            view_item.tags = tags
            view_item._initial_tags = copy.copy(tags)
        if (location_elem := view_xml.find(".//t:location", namespaces=ns)) is not None:
            location = LocationItem.from_xml(location_elem, ns)
            view_item._location = location
        if data_acceleration_config_elem is not None:
            data_acceleration_config = parse_data_acceleration_config(data_acceleration_config_elem)
            view_item.data_acceleration_config = data_acceleration_config
        return view_item


def parse_data_acceleration_config(data_acceleration_elem):
    data_acceleration_config = dict()

    acceleration_enabled = data_acceleration_elem.get("accelerationEnabled", None)
    if acceleration_enabled is not None:
        acceleration_enabled = string_to_bool(acceleration_enabled)

    acceleration_status = data_acceleration_elem.get("accelerationStatus", None)

    data_acceleration_config["acceleration_enabled"] = acceleration_enabled
    data_acceleration_config["acceleration_status"] = acceleration_status
    return data_acceleration_config


def string_to_bool(s: str) -> bool:
    return s.lower() == "true"


@overload
def string_to_int(s: None) -> None: ...


@overload
def string_to_int(s: str) -> int: ...


def string_to_int(s):
    return int(s) if s is not None else None
