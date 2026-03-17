import copy
import datetime
import xml.etree.ElementTree as ET
from typing import Optional

from defusedxml.ElementTree import fromstring

from tableauserverclient.datetime_helpers import parse_datetime
from tableauserverclient.helpers.strings import nullable_str_to_bool, nullable_str_to_int
from tableauserverclient.models.connection_item import ConnectionItem
from tableauserverclient.models.exceptions import UnpopulatedPropertyError
from tableauserverclient.models.permissions_item import PermissionsRule
from tableauserverclient.models.project_item import ProjectItem
from tableauserverclient.models.property_decorators import (
    property_not_nullable,
    property_is_boolean,
    property_is_enum,
)
from tableauserverclient.models.revision_item import RevisionItem
from tableauserverclient.models.tag_item import TagItem
from tableauserverclient.models.user_item import UserItem


class DatasourceItem:
    """
    Represents a Tableau datasource item.

    Parameters
    ----------
    project_id : Optional[str]
        The project ID that the datasource belongs to.

    name : Optional[str]
        The name of the datasource.

    Attributes
    ----------
    ask_data_enablement : Optional[str]
        Determines if a data source allows use of Ask Data. The value can be
        TSC.DatasourceItem.AskDataEnablement.Enabled,
        TSC.DatasourceItem.AskDataEnablement.Disabled, or
        TSC.DatasourceItem.AskDataEnablement.SiteDefault. If no setting is
        specified, it will default to SiteDefault. See REST API Publish
        Datasource for more information about ask_data_enablement.

    connected_workbooks_count : Optional[int]
        The number of workbooks connected to the datasource.

    connections : list[ConnectionItem]
        The list of data connections (ConnectionItem) for the specified data
        source. You must first call the populate_connections method to access
        this data. See the ConnectionItem class.

    content_url : Optional[str]
        The name of the data source as it would appear in a URL.

    created_at : Optional[datetime.datetime]
        The time the data source was created.

    certified : Optional[bool]
        A Boolean value that indicates whether the data source is certified.

    certification_note : Optional[str]
        The optional note that describes the certified data source.

    datasource_type : Optional[str]
        The type of data source, for example, sqlserver or excel-direct.

    description : Optional[str]
        The description for the data source.

    encrypt_extracts : Optional[bool]
        A Boolean value to determine if a datasource should be encrypted or not.
        See Extract and Encryption Methods for more information.

    favorites_total : Optional[int]
        The number of users who have marked the data source as a favorite.

    has_alert : Optional[bool]
        A Boolean value that indicates whether the data source has an alert.

    has_extracts : Optional[bool]
        A Boolean value that indicates whether the datasource has extracts.

    id : Optional[str]
        The identifier for the data source. You need this value to query a
        specific data source or to delete a data source with the get_by_id and
        delete methods.

    is_published : Optional[bool]
        A Boolean value that indicates whether the data source is published.

    name : Optional[str]
        The name of the data source. If not specified, the name of the published
        data source file is used.

    owner: Optional[UserItem]
        The owner of the data source.

    owner_id : Optional[str]
        The identifier of the owner of the data source.

    project : Optional[ProjectItem]
        The project that the data source belongs to.

    project_id : Optional[str]
        The identifier of the project associated with the data source. You must
        provide this identifier when you create an instance of a DatasourceItem.

    project_name : Optional[str]
        The name of the project associated with the data source.

    server_name : Optional[str]
        The name of the server where the data source is published.

    tags : Optional[set[str]]
        The tags (list of strings) that have been added to the data source.

    updated_at : Optional[datetime.datetime]
        The date and time when the data source was last updated.

    use_remote_query_agent : Optional[bool]
        A Boolean value that indicates whether to allow or disallow your Tableau
        Cloud site to use Tableau Bridge clients. Bridge allows you to maintain
        data sources with live connections to supported on-premises data
        sources. See Configure and Manage the Bridge Client Pool for more
        information.

    webpage_url : Optional[str]
        The url of the datasource as displayed in browsers.
    """

    class AskDataEnablement:
        Enabled = "Enabled"
        Disabled = "Disabled"
        SiteDefault = "SiteDefault"

    def __repr__(self):
        return "<Datasource {} '{}' ({} parent={} >".format(
            self._id,
            self.name,
            self.description or "No Description",
            self.project_id,
        )

    def __init__(self, project_id: Optional[str] = None, name: Optional[str] = None) -> None:
        self._ask_data_enablement: Optional[str] = None
        self._certified: Optional[bool] = None
        self._certification_note: Optional[str] = None
        self._connections: Optional[list[ConnectionItem]] = None
        self._content_url: Optional[str] = None
        self._created_at: Optional[datetime.datetime] = None
        self._datasource_type: Optional[str] = None
        self._description: Optional[str] = None
        self._encrypt_extracts: Optional[bool] = None
        self._has_extracts: Optional[bool] = None
        self._id: Optional[str] = None
        self._initial_tags: set = set()
        self._project_name: Optional[str] = None
        self._revisions = None
        self._size: Optional[int] = None
        self._updated_at: Optional[datetime.datetime] = None
        self._use_remote_query_agent: Optional[bool] = None
        self._webpage_url: Optional[str] = None
        self.description: Optional[str] = None
        self.name: Optional[str] = name
        self.owner_id: Optional[str] = None
        self.project_id: Optional[str] = project_id
        self.tags: set[str] = set()
        self._connected_workbooks_count: Optional[int] = None
        self._favorites_total: Optional[int] = None
        self._has_alert: Optional[bool] = None
        self._is_published: Optional[bool] = None
        self._server_name: Optional[str] = None
        self._project: Optional[ProjectItem] = None
        self._owner: Optional[UserItem] = None

        self._permissions = None
        self._data_quality_warnings = None

        return None

    @property
    def ask_data_enablement(self) -> Optional[str]:
        return self._ask_data_enablement

    @ask_data_enablement.setter
    @property_is_enum(AskDataEnablement)
    def ask_data_enablement(self, value: Optional[str]):
        self._ask_data_enablement = value

    @property
    def connections(self):
        if self._connections is None:
            error = "Datasource item must be populated with connections first."
            raise UnpopulatedPropertyError(error)
        return self._connections()

    @property
    def permissions(self) -> Optional[list[PermissionsRule]]:
        if self._permissions is None:
            error = "Project item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._permissions()

    @property
    def content_url(self) -> Optional[str]:
        return self._content_url

    @property
    def created_at(self) -> Optional[datetime.datetime]:
        return self._created_at

    @property
    def certified(self) -> Optional[bool]:
        return self._certified

    @certified.setter
    @property_not_nullable
    @property_is_boolean
    def certified(self, value: Optional[bool]):
        self._certified = value

    @property
    def certification_note(self) -> Optional[str]:
        return self._certification_note

    @certification_note.setter
    def certification_note(self, value: Optional[str]):
        self._certification_note = value

    @property
    def encrypt_extracts(self) -> Optional[bool]:
        return self._encrypt_extracts

    @encrypt_extracts.setter
    @property_is_boolean
    def encrypt_extracts(self, value: Optional[bool]):
        self._encrypt_extracts = value

    @property
    def dqws(self):
        if self._data_quality_warnings is None:
            error = "Project item must be populated with dqws first."
            raise UnpopulatedPropertyError(error)
        return self._data_quality_warnings()

    @property
    def has_extracts(self) -> Optional[bool]:
        return self._has_extracts

    @property
    def id(self) -> Optional[str]:
        return self._id

    @property
    def project_id(self) -> Optional[str]:
        return self._project_id

    @project_id.setter
    def project_id(self, value: Optional[str]):
        self._project_id = value

    @property
    def project_name(self) -> Optional[str]:
        return self._project_name

    @property
    def datasource_type(self) -> Optional[str]:
        return self._datasource_type

    @property
    def description(self) -> Optional[str]:
        return self._description

    @description.setter
    def description(self, value: Optional[str]):
        self._description = value

    @property
    def updated_at(self) -> Optional[datetime.datetime]:
        return self._updated_at

    @property
    def use_remote_query_agent(self) -> Optional[bool]:
        return self._use_remote_query_agent

    @use_remote_query_agent.setter
    @property_is_boolean
    def use_remote_query_agent(self, value: bool):
        self._use_remote_query_agent = value

    @property
    def webpage_url(self) -> Optional[str]:
        return self._webpage_url

    @property
    def revisions(self) -> list[RevisionItem]:
        if self._revisions is None:
            error = "Datasource item must be populated with revisions first."
            raise UnpopulatedPropertyError(error)
        return self._revisions()

    @property
    def size(self) -> Optional[int]:
        return self._size

    @property
    def connected_workbooks_count(self) -> Optional[int]:
        return self._connected_workbooks_count

    @property
    def favorites_total(self) -> Optional[int]:
        return self._favorites_total

    @property
    def has_alert(self) -> Optional[bool]:
        return self._has_alert

    @property
    def is_published(self) -> Optional[bool]:
        return self._is_published

    @property
    def server_name(self) -> Optional[str]:
        return self._server_name

    @property
    def project(self) -> Optional[ProjectItem]:
        return self._project

    @property
    def owner(self) -> Optional[UserItem]:
        return self._owner

    def _set_connections(self, connections) -> None:
        self._connections = connections

    def _set_permissions(self, permissions):
        self._permissions = permissions

    def _set_data_quality_warnings(self, dqw):
        self._data_quality_warnings = dqw

    def _set_revisions(self, revisions):
        self._revisions = revisions

    def _parse_common_elements(self, datasource_xml, ns):
        if not isinstance(datasource_xml, ET.Element):
            datasource_xml = fromstring(datasource_xml).find(".//t:datasource", namespaces=ns)
        if datasource_xml is not None:
            (
                ask_data_enablement,
                certified,
                certification_note,
                _,
                _,
                _,
                _,
                encrypt_extracts,
                has_extracts,
                _,
                _,
                owner_id,
                project_id,
                project_name,
                _,
                updated_at,
                use_remote_query_agent,
                webpage_url,
                size,
                connected_workbooks_count,
                favorites_total,
                has_alert,
                is_published,
                server_name,
                project,
                owner,
            ) = self._parse_element(datasource_xml, ns)
            self._set_values(
                ask_data_enablement,
                certified,
                certification_note,
                None,
                None,
                None,
                None,
                encrypt_extracts,
                has_extracts,
                None,
                None,
                owner_id,
                project_id,
                project_name,
                None,
                updated_at,
                use_remote_query_agent,
                webpage_url,
                size,
                connected_workbooks_count,
                favorites_total,
                has_alert,
                is_published,
                server_name,
                project,
                owner,
            )
        return self

    def _set_values(
        self,
        ask_data_enablement,
        certified,
        certification_note,
        content_url,
        created_at,
        datasource_type,
        description,
        encrypt_extracts,
        has_extracts,
        id_,
        name,
        owner_id,
        project_id,
        project_name,
        tags,
        updated_at,
        use_remote_query_agent,
        webpage_url,
        size,
        connected_workbooks_count,
        favorites_total,
        has_alert,
        is_published,
        server_name,
        project,
        owner,
    ):
        if ask_data_enablement is not None:
            self._ask_data_enablement = ask_data_enablement
        if certification_note:
            self.certification_note = certification_note
        self.certified = certified  # Always True/False, not conditional
        if content_url:
            self._content_url = content_url
        if created_at:
            self._created_at = created_at
        if datasource_type:
            self._datasource_type = datasource_type
        if description:
            self.description = description
        if encrypt_extracts is not None:
            self.encrypt_extracts = str(encrypt_extracts).lower() == "true"
        if has_extracts is not None:
            self._has_extracts = str(has_extracts).lower() == "true"
        if id_ is not None:
            self._id = id_
        if name:
            self.name = name
        if owner_id:
            self.owner_id = owner_id
        if project_id:
            self.project_id = project_id
        if project_name:
            self._project_name = project_name
        if tags:
            self.tags = tags
            self._initial_tags = copy.copy(tags)
        if updated_at:
            self._updated_at = updated_at
        if use_remote_query_agent is not None:
            self._use_remote_query_agent = str(use_remote_query_agent).lower() == "true"
        if webpage_url:
            self._webpage_url = webpage_url
        if size is not None:
            self._size = int(size)
        if connected_workbooks_count is not None:
            self._connected_workbooks_count = connected_workbooks_count
        if favorites_total is not None:
            self._favorites_total = favorites_total
        if has_alert is not None:
            self._has_alert = has_alert
        if is_published is not None:
            self._is_published = is_published
        if server_name is not None:
            self._server_name = server_name
        if project is not None:
            self._project = project
        if owner is not None:
            self._owner = owner

    @classmethod
    def from_response(cls, resp: bytes, ns: dict) -> list["DatasourceItem"]:
        all_datasource_items = list()
        parsed_response = fromstring(resp)
        all_datasource_xml = parsed_response.findall(".//t:datasource", namespaces=ns)

        for datasource_xml in all_datasource_xml:
            datasource_item = cls.from_xml(datasource_xml, ns)
            all_datasource_items.append(datasource_item)
        return all_datasource_items

    @classmethod
    def from_xml(cls, datasource_xml, ns):
        datasource_item = cls()
        datasource_item._set_values(*cls._parse_element(datasource_xml, ns))
        return datasource_item

    @staticmethod
    def _parse_element(datasource_xml: ET.Element, ns: dict) -> tuple:
        id_ = datasource_xml.get("id", None)
        name = datasource_xml.get("name", None)
        datasource_type = datasource_xml.get("type", None)
        description = datasource_xml.get("description", None)
        content_url = datasource_xml.get("contentUrl", None)
        created_at = parse_datetime(datasource_xml.get("createdAt", None))
        updated_at = parse_datetime(datasource_xml.get("updatedAt", None))
        certification_note = datasource_xml.get("certificationNote", None)
        certified = str(datasource_xml.get("isCertified", None)).lower() == "true"
        encrypt_extracts = datasource_xml.get("encryptExtracts", None)
        has_extracts = datasource_xml.get("hasExtracts", None)
        use_remote_query_agent = datasource_xml.get("useRemoteQueryAgent", None)
        webpage_url = datasource_xml.get("webpageUrl", None)
        size = datasource_xml.get("size", None)
        connected_workbooks_count = nullable_str_to_int(datasource_xml.get("connectedWorkbooksCount", None))
        favorites_total = nullable_str_to_int(datasource_xml.get("favoritesTotal", None))
        has_alert = nullable_str_to_bool(datasource_xml.get("hasAlert", None))
        is_published = nullable_str_to_bool(datasource_xml.get("isPublished", None))
        server_name = datasource_xml.get("serverName", None)

        tags = None
        tags_elem = datasource_xml.find(".//t:tags", namespaces=ns)
        if tags_elem is not None:
            tags = TagItem.from_xml_element(tags_elem, ns)

        project_id = None
        project_name = None
        project = None
        project_elem = datasource_xml.find(".//t:project", namespaces=ns)
        if project_elem is not None:
            project = ProjectItem.from_xml(project_elem, ns)
            project_id = project_elem.get("id", None)
            project_name = project_elem.get("name", None)

        owner_id = None
        owner = None
        owner_elem = datasource_xml.find(".//t:owner", namespaces=ns)
        if owner_elem is not None:
            owner = UserItem.from_xml(owner_elem, ns)
            owner_id = owner_elem.get("id", None)

        ask_data_enablement = None
        ask_data_elem = datasource_xml.find(".//t:askData", namespaces=ns)
        if ask_data_elem is not None:
            ask_data_enablement = ask_data_elem.get("enablement", None)

        return (
            ask_data_enablement,
            certified,
            certification_note,
            content_url,
            created_at,
            datasource_type,
            description,
            encrypt_extracts,
            has_extracts,
            id_,
            name,
            owner_id,
            project_id,
            project_name,
            tags,
            updated_at,
            use_remote_query_agent,
            webpage_url,
            size,
            connected_workbooks_count,
            favorites_total,
            has_alert,
            is_published,
            server_name,
            project,
            owner,
        )
