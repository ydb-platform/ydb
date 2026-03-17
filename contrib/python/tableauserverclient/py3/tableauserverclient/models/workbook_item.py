import copy
import datetime
import uuid
import xml.etree.ElementTree as ET
from typing import Callable, Optional, overload

from defusedxml.ElementTree import fromstring

from tableauserverclient.datetime_helpers import parse_datetime
from tableauserverclient.models.location_item import LocationItem
from tableauserverclient.models.project_item import ProjectItem
from tableauserverclient.models.user_item import UserItem
from .connection_item import ConnectionItem
from .exceptions import UnpopulatedPropertyError
from .permissions_item import PermissionsRule
from .property_decorators import (
    property_is_boolean,
    property_is_data_acceleration_config,
)
from .revision_item import RevisionItem
from .tag_item import TagItem
from .view_item import ViewItem
from .data_freshness_policy_item import DataFreshnessPolicyItem


class WorkbookItem:
    """
    The workbook resources for Tableau are defined in the WorkbookItem class.
    The class corresponds to the workbook resources you can access using the
    Tableau REST API. Some workbook methods take an instance of the WorkbookItem
    class as arguments. The workbook item specifies the project.

    Parameters
    ----------
    project_id : Optional[str], optional
        The project ID for the workbook, by default None.

    name : Optional[str], optional
        The name of the workbook, by default None.

    show_tabs : bool, optional
        Determines whether the workbook shows tabs for the view.

    Attributes
    ----------
    connections : list[ConnectionItem]
        The list of data connections (ConnectionItem) for the data sources used
        by the workbook. You must first call the workbooks.populate_connections
        method to access this data. See the ConnectionItem class.

    content_url : Optional[str]
        The name of the workbook as it appears in the URL.

    created_at : Optional[datetime.datetime]
        The date and time the workbook was created.

    default_view_id : Optional[str]
        The identifier for the default view of the workbook.

    description : Optional[str]
        User-defined description of the workbook.

    encrypt_extracts : Optional[bool]
        Indicates whether extracts are encrypted.

    has_extracts : Optional[bool]
        Indicates whether the workbook has extracts.

    id : Optional[str]
       The identifier for the workbook. You need this value to query a specific
       workbook or to delete a workbook with the get_by_id and delete methods.

    last_published_at : Optional[datetime.datetime]
        The date and time the workbook was last published.

    location : Optional[LocationItem]
        The location of the workbook, such as a personal space or project.

    owner : Optional[UserItem]
        The owner of the workbook.

    owner_id : Optional[str]
        The identifier for the owner (UserItem) of the workbook.

    preview_image : bytes
        The thumbnail image for the view. You must first call the
        workbooks.populate_preview_image method to access this data.

    project: Optional[ProjectItem]
        The project that contains the workbook.

    project_name : Optional[str]
        The name of the project that contains the workbook.

    size: int
        The size of the workbook in megabytes.

    hidden_views: Optional[list[str]]
        List of string names of views that need to be hidden when the workbook
        is published.

    tags: set[str]
        The set of tags associated with the workbook.

    updated_at : Optional[datetime.datetime]
        The date and time the workbook was last updated.

    views : list[ViewItem]
        The list of views (ViewItem) for the workbook. You must first call the
        workbooks.populate_views method to access this data. See the ViewItem
        class.

    web_page_url : Optional[str]
        The full URL for the workbook.

    Examples
    --------
    # creating a new instance of a WorkbookItem
    >>> import tableauserverclient as TSC

    >>> # Create new workbook_item with project id '3a8b6148-493c-11e6-a621-6f3499394a39'

    >>> new_workbook = TSC.WorkbookItem('3a8b6148-493c-11e6-a621-6f3499394a39')
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        name: Optional[str] = None,
        show_tabs: bool = False,
        thumbnails_user_id: Optional[str] = None,
        thumbnails_group_id: Optional[str] = None,
    ) -> None:
        self._connections = None
        self._content_url = None
        self._webpage_url = None
        self._created_at = None
        self._id: Optional[str] = None
        self._initial_tags: set = set()
        self._pdf = None
        self._powerpoint = None
        self._preview_image = None
        self._project_name = None
        self._revisions = None
        self._size = None
        self._updated_at = None
        self._views: Optional[Callable[[], list[ViewItem]]] = None
        self.name = name
        self._description = None
        self.owner_id: Optional[str] = None
        # workaround for Personal Space workbooks without a project
        self.project_id: Optional[str] = project_id or uuid.uuid4().__str__()
        self.show_tabs = show_tabs
        self.hidden_views: Optional[list[str]] = None
        self.tags: set[str] = set()
        self.data_acceleration_config = {
            "acceleration_enabled": None,
            "accelerate_now": None,
            "last_updated_at": None,
            "acceleration_status": None,
        }
        self.data_freshness_policy = None
        self._permissions = None
        self.thumbnails_user_id = thumbnails_user_id
        self.thumbnails_group_id = thumbnails_group_id
        self._sheet_count: Optional[int] = None
        self._has_extracts: Optional[bool] = None
        self._project: Optional[ProjectItem] = None
        self._owner: Optional[UserItem] = None
        self._location: Optional[LocationItem] = None
        self._encrypt_extracts: Optional[bool] = None
        self._default_view_id: Optional[str] = None
        self._share_description: Optional[str] = None
        self._last_published_at: Optional[datetime.datetime] = None

        return None

    def __str__(self):
        return "<WorkbookItem {} '{}' contentUrl='{}' project={}>".format(
            self._id, self.name, self.content_url, self.project_id
        )

    def __repr__(self):
        return self.__str__() + "  { " + ", ".join(" % s: % s" % item for item in vars(self).items()) + "}"

    @property
    def connections(self) -> list[ConnectionItem]:
        if self._connections is None:
            error = "Workbook item must be populated with connections first."
            raise UnpopulatedPropertyError(error)
        return self._connections()

    @property
    def permissions(self) -> list[PermissionsRule]:
        if self._permissions is None:
            error = "Workbook item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._permissions()

    @property
    def content_url(self) -> Optional[str]:
        return self._content_url

    @property
    def webpage_url(self) -> Optional[str]:
        return self._webpage_url

    @property
    def created_at(self) -> Optional[datetime.datetime]:
        return self._created_at

    @property
    def description(self) -> Optional[str]:
        return self._description

    @description.setter
    def description(self, value: str):
        self._description = value

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def powerpoint(self) -> bytes:
        if self._powerpoint is None:
            error = "Workbook item must be populated with its powerpoint first."
            raise UnpopulatedPropertyError(error)
        return self._powerpoint()

    @property
    def pdf(self) -> bytes:
        if self._pdf is None:
            error = "Workbook item must be populated with its pdf first."
            raise UnpopulatedPropertyError(error)
        return self._pdf()

    @property
    def preview_image(self) -> bytes:
        if self._preview_image is None:
            error = "Workbook item must be populated with its preview image first."
            raise UnpopulatedPropertyError(error)
        return self._preview_image()

    @property
    def project_id(self) -> Optional[str]:
        return self._project_id

    @project_id.setter
    def project_id(self, value: str):
        self._project_id = value

    @property
    def project_name(self) -> Optional[str]:
        return self._project_name

    @property
    def show_tabs(self) -> bool:
        return self._show_tabs

    @show_tabs.setter
    @property_is_boolean
    def show_tabs(self, value: bool):
        self._show_tabs = value

    @property
    def size(self):
        return self._size

    @property
    def sheet_count(self) -> Optional[int]:
        return self._sheet_count

    @property
    def has_extracts(self) -> Optional[bool]:
        return self._has_extracts

    @property
    def updated_at(self) -> Optional[datetime.datetime]:
        return self._updated_at

    @property
    def views(self) -> list[ViewItem]:
        # Views can be set in an initial workbook response OR by a call
        # to Server. Without getting too fancy, I think we can rely on
        # returning a list from the response, until they call
        # populate_workbook, in which case we bind the fetcher and
        # return a callable.
        if self._views is None:
            error = "Workbook item must be populated with views first."
            raise UnpopulatedPropertyError(error)
        elif callable(self._views):
            # We've called `populate_views` on this model
            return self._views()
        else:
            # We had views included in a WorkbookItem response
            return self._views

    @views.setter
    def views(self, value):
        self._views = value

    @property
    def data_acceleration_config(self):
        return self._data_acceleration_config

    @data_acceleration_config.setter
    @property_is_data_acceleration_config
    def data_acceleration_config(self, value):
        self._data_acceleration_config = value

    @property
    def data_freshness_policy(self):
        return self._data_freshness_policy

    @data_freshness_policy.setter
    # @property_is_data_freshness_policy
    def data_freshness_policy(self, value):
        self._data_freshness_policy = value

    @property
    def revisions(self) -> list[RevisionItem]:
        if self._revisions is None:
            error = "Workbook item must be populated with revisions first."
            raise UnpopulatedPropertyError(error)
        return self._revisions()

    @property
    def thumbnails_user_id(self) -> Optional[str]:
        return self._thumbnails_user_id

    @thumbnails_user_id.setter
    def thumbnails_user_id(self, value: Optional[str]):
        self._thumbnails_user_id = value

    @property
    def thumbnails_group_id(self) -> Optional[str]:
        return self._thumbnails_group_id

    @thumbnails_group_id.setter
    def thumbnails_group_id(self, value: Optional[str]):
        self._thumbnails_group_id = value

    @property
    def project(self) -> Optional[ProjectItem]:
        return self._project

    @property
    def owner(self) -> Optional[UserItem]:
        return self._owner

    @property
    def location(self) -> Optional[LocationItem]:
        return self._location

    @property
    def encrypt_extracts(self) -> Optional[bool]:
        return self._encrypt_extracts

    @property
    def default_view_id(self) -> Optional[str]:
        return self._default_view_id

    @property
    def share_description(self) -> Optional[str]:
        return self._share_description

    @property
    def last_published_at(self) -> Optional[datetime.datetime]:
        return self._last_published_at

    def _set_connections(self, connections):
        self._connections = connections

    def _set_permissions(self, permissions):
        self._permissions = permissions

    def _set_views(self, views: Callable[[], list[ViewItem]]) -> None:
        self._views = views

    def _set_pdf(self, pdf: Callable[[], bytes]) -> None:
        self._pdf = pdf

    def _set_powerpoint(self, pptx: Callable[[], bytes]) -> None:
        self._powerpoint = pptx

    def _set_preview_image(self, preview_image: Callable[[], bytes]) -> None:
        self._preview_image = preview_image

    def _set_revisions(self, revisions):
        self._revisions = revisions

    def _parse_common_tags(self, workbook_xml, ns):
        if not isinstance(workbook_xml, ET.Element):
            workbook_xml = fromstring(workbook_xml).find(".//t:workbook", namespaces=ns)
        if workbook_xml is not None:
            (
                _,
                _,
                _,
                _,
                _,
                description,
                updated_at,
                _,
                show_tabs,
                project_id,
                project_name,
                owner_id,
                _,
                views,
                data_acceleration_config,
                data_freshness_policy,
                sheet_count,
                has_extracts,
                project,
                owner,
                location,
                encrypt_extracts,
                default_view_id,
                share_description,
                last_published_at,
            ) = self._parse_element(workbook_xml, ns)

            self._set_values(
                None,
                None,
                None,
                None,
                None,
                description,
                updated_at,
                None,
                show_tabs,
                project_id,
                project_name,
                owner_id,
                None,
                views,
                data_acceleration_config,
                data_freshness_policy,
                sheet_count,
                has_extracts,
                project,
                owner,
                location,
                encrypt_extracts,
                default_view_id,
                share_description,
                last_published_at,
            )

        return self

    def _set_values(
        self,
        id,
        name,
        content_url,
        webpage_url,
        created_at,
        description,
        updated_at,
        size,
        show_tabs,
        project_id,
        project_name,
        owner_id,
        tags,
        views,
        data_acceleration_config,
        data_freshness_policy,
        sheet_count,
        has_extracts,
        project,
        owner,
        location,
        encrypt_extracts,
        default_view_id,
        share_description,
        last_published_at,
    ):
        if id is not None:
            self._id = id
        if name:
            self.name = name
        if content_url:
            self._content_url = content_url
        if webpage_url:
            self._webpage_url = webpage_url
        if created_at:
            self._created_at = created_at
        if description:
            self._description = description
        if updated_at:
            self._updated_at = updated_at
        if size:
            self._size = size
        if show_tabs:
            self._show_tabs = show_tabs
        if project_id:
            self.project_id = project_id
        if project_name:
            self._project_name = project_name
        if owner_id:
            self.owner_id = owner_id
        if tags:
            self.tags = tags
            self._initial_tags = copy.copy(tags)
        if views is not None:
            self._views = views
        if data_acceleration_config is not None:
            self.data_acceleration_config = data_acceleration_config
        if data_freshness_policy is not None:
            self.data_freshness_policy = data_freshness_policy
        if sheet_count is not None:
            self._sheet_count = sheet_count
        if has_extracts is not None:
            self._has_extracts = has_extracts
        if project:
            self._project = project
        if owner:
            self._owner = owner
        if location:
            self._location = location
        if encrypt_extracts is not None:
            self._encrypt_extracts = encrypt_extracts
        if default_view_id is not None:
            self._default_view_id = default_view_id
        if share_description is not None:
            self._share_description = share_description
        if last_published_at is not None:
            self._last_published_at = last_published_at

    @classmethod
    def from_response(cls, resp: str, ns: dict[str, str]) -> list["WorkbookItem"]:
        all_workbook_items = list()
        parsed_response = fromstring(resp)
        all_workbook_xml = parsed_response.findall(".//t:workbook", namespaces=ns)
        for workbook_xml in all_workbook_xml:
            workbook_item = cls.from_xml(workbook_xml, ns)
            all_workbook_items.append(workbook_item)
        return all_workbook_items

    @classmethod
    def from_xml(cls, workbook_xml, ns):
        workbook_item = cls()
        workbook_item._set_values(*cls._parse_element(workbook_xml, ns))
        return workbook_item

    @staticmethod
    def _parse_element(workbook_xml, ns):
        id = workbook_xml.get("id", None)
        name = workbook_xml.get("name", None)
        content_url = workbook_xml.get("contentUrl", None)
        webpage_url = workbook_xml.get("webpageUrl", None)
        created_at = parse_datetime(workbook_xml.get("createdAt", None))
        description = workbook_xml.get("description", None)
        updated_at = parse_datetime(workbook_xml.get("updatedAt", None))
        sheet_count = string_to_int(workbook_xml.get("sheetCount", None))
        has_extracts = string_to_bool(workbook_xml.get("hasExtracts", ""))
        encrypt_extracts = string_to_bool(e) if (e := workbook_xml.get("encryptExtracts", None)) is not None else None
        default_view_id = workbook_xml.get("defaultViewId", None)
        share_description = workbook_xml.get("shareDescription", None)
        last_published_at = parse_datetime(workbook_xml.get("lastPublishedAt", None))

        size = workbook_xml.get("size", None)
        if size:
            size = int(size)

        show_tabs = string_to_bool(workbook_xml.get("showTabs", ""))

        project_id = None
        project_name = None
        project = None
        project_tag = workbook_xml.find(".//t:project", namespaces=ns)
        if project_tag is not None:
            project = ProjectItem.from_xml(project_tag, ns)
            project_id = project_tag.get("id", None)
            project_name = project_tag.get("name", None)

        owner_id = None
        owner = None
        owner_tag = workbook_xml.find(".//t:owner", namespaces=ns)
        if owner_tag is not None:
            owner = UserItem.from_xml(owner_tag, ns)
            owner_id = owner_tag.get("id", None)

        tags = None
        tags_elem = workbook_xml.find(".//t:tags", namespaces=ns)
        if tags_elem is not None:
            all_tags = TagItem.from_xml_element(tags_elem, ns)
            tags = all_tags

        views = None
        views_elem = workbook_xml.find(".//t:views", namespaces=ns)
        if views_elem is not None:
            views = ViewItem.from_xml_element(views_elem, ns)

        location = None
        location_elem = workbook_xml.find(".//t:location", namespaces=ns)
        if location_elem is not None:
            location = LocationItem.from_xml(location_elem, ns)

        data_acceleration_config = {
            "acceleration_enabled": None,
            "accelerate_now": None,
            "last_updated_at": None,
            "acceleration_status": None,
        }
        data_acceleration_elem = workbook_xml.find(".//t:dataAccelerationConfig", namespaces=ns)
        if data_acceleration_elem is not None:
            data_acceleration_config = parse_data_acceleration_config(data_acceleration_elem)

        data_freshness_policy = None
        data_freshness_policy_elem = workbook_xml.find(".//t:dataFreshnessPolicy", namespaces=ns)
        if data_freshness_policy_elem is not None:
            data_freshness_policy = DataFreshnessPolicyItem.from_xml_element(data_freshness_policy_elem, ns)

        return (
            id,
            name,
            content_url,
            webpage_url,
            created_at,
            description,
            updated_at,
            size,
            show_tabs,
            project_id,
            project_name,
            owner_id,
            tags,
            views,
            data_acceleration_config,
            data_freshness_policy,
            sheet_count,
            has_extracts,
            project,
            owner,
            location,
            encrypt_extracts,
            default_view_id,
            share_description,
            last_published_at,
        )


def parse_data_acceleration_config(data_acceleration_elem):
    data_acceleration_config = dict()

    acceleration_enabled = data_acceleration_elem.get("accelerationEnabled", None)
    if acceleration_enabled is not None:
        acceleration_enabled = string_to_bool(acceleration_enabled)

    accelerate_now = data_acceleration_elem.get("accelerateNow", None)
    if accelerate_now is not None:
        accelerate_now = string_to_bool(accelerate_now)

    last_updated_at = data_acceleration_elem.get("lastUpdatedAt", None)
    if last_updated_at is not None:
        last_updated_at = parse_datetime(last_updated_at)

    acceleration_status = data_acceleration_elem.get("accelerationStatus", None)

    data_acceleration_config["acceleration_enabled"] = acceleration_enabled
    data_acceleration_config["accelerate_now"] = accelerate_now
    data_acceleration_config["last_updated_at"] = last_updated_at
    data_acceleration_config["acceleration_status"] = acceleration_status
    return data_acceleration_config


# Used to convert string represented boolean to a boolean type
def string_to_bool(s: str) -> bool:
    return s.lower() == "true"


@overload
def string_to_int(s: None) -> None: ...


@overload
def string_to_int(s: str) -> int: ...


def string_to_int(s):
    return int(s) if s is not None else None
