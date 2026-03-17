import xml.etree.ElementTree as ET
from .exceptions import UnpopulatedPropertyError
from .property_decorators import property_not_nullable
from .tag_item import TagItem
from ..datetime_helpers import parse_datetime
import copy


class FlowItem(object):
    def __init__(self, project_id, name=None):
        self._webpage_url = None
        self._created_at = None
        self._id = None
        self._initial_tags = set()
        self._project_name = None
        self._updated_at = None
        self.name = name
        self.owner_id = None
        self.project_id = project_id
        self.tags = set()
        self.description = None

        self._connections = None
        self._permissions = None

    @property
    def connections(self):
        if self._connections is None:
            error = 'Flow item must be populated with connections first.'
            raise UnpopulatedPropertyError(error)
        return self._connections()

    @property
    def permissions(self):
        if self._permissions is None:
            error = "Project item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._permissions()

    @property
    def webpage_url(self):
        return self._webpage_url

    @property
    def created_at(self):
        return self._created_at

    @property
    def id(self):
        return self._id

    @property
    def project_id(self):
        return self._project_id

    @project_id.setter
    @property_not_nullable
    def project_id(self, value):
        self._project_id = value

    @property
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    @property
    def project_name(self):
        return self._project_name

    @property
    def flow_type(self):
        return self._flow_type

    @property
    def updated_at(self):
        return self._updated_at

    def _set_connections(self, connections):
        self._connections = connections

    def _set_permissions(self, permissions):
        self._permissions = permissions

    def _parse_common_elements(self, flow_xml, ns):
        if not isinstance(flow_xml, ET.Element):
            flow_xml = ET.fromstring(flow_xml).find('.//t:flow', namespaces=ns)
        if flow_xml is not None:
            (_, _, _, _, _, updated_at, _, project_id, project_name, owner_id) = self._parse_element(flow_xml, ns)
            self._set_values(None, None, None, None, None, updated_at, None, project_id,
                             project_name, owner_id)
        return self

    def _set_values(self, id, name, description, webpage_url, created_at,
                    updated_at, tags, project_id, project_name, owner_id):
        if id is not None:
            self._id = id
        if name:
            self.name = name
        if description:
            self.description = description
        if webpage_url:
            self._webpage_url = webpage_url
        if created_at:
            self._created_at = created_at
        if updated_at:
            self._updated_at = updated_at
        if tags:
            self.tags = tags
            self._initial_tags = copy.copy(tags)
        if project_id:
            self.project_id = project_id
        if project_name:
            self._project_name = project_name
        if owner_id:
            self.owner_id = owner_id

    @classmethod
    def from_response(cls, resp, ns):
        all_flow_items = list()
        parsed_response = ET.fromstring(resp)
        all_flow_xml = parsed_response.findall('.//t:flow', namespaces=ns)

        for flow_xml in all_flow_xml:
            (id_, name, description, webpage_url, created_at, updated_at,
             tags, project_id, project_name, owner_id) = cls._parse_element(flow_xml, ns)
            flow_item = cls(project_id)
            flow_item._set_values(id_, name, description, webpage_url, created_at, updated_at,
                                  tags, None, project_name, owner_id)
            all_flow_items.append(flow_item)
        return all_flow_items

    @staticmethod
    def _parse_element(flow_xml, ns):
        id_ = flow_xml.get('id', None)
        name = flow_xml.get('name', None)
        description = flow_xml.get('description', None)
        webpage_url = flow_xml.get('webpageUrl', None)
        created_at = parse_datetime(flow_xml.get('createdAt', None))
        updated_at = parse_datetime(flow_xml.get('updatedAt', None))

        tags = None
        tags_elem = flow_xml.find('.//t:tags', namespaces=ns)
        if tags_elem is not None:
            tags = TagItem.from_xml_element(tags_elem, ns)

        project_id = None
        project_name = None
        project_elem = flow_xml.find('.//t:project', namespaces=ns)
        if project_elem is not None:
            project_id = project_elem.get('id', None)
            project_name = project_elem.get('name', None)

        owner_id = None
        owner_elem = flow_xml.find('.//t:owner', namespaces=ns)
        if owner_elem is not None:
            owner_id = owner_elem.get('id', None)

        return (id_, name, description, webpage_url, created_at, updated_at, tags, project_id,
                project_name, owner_id)
