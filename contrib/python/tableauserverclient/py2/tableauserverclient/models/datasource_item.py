import xml.etree.ElementTree as ET
from .exceptions import UnpopulatedPropertyError
from .property_decorators import property_not_nullable, property_is_boolean
from .tag_item import TagItem
from ..datetime_helpers import parse_datetime
import copy


class DatasourceItem(object):
    def __init__(self, project_id, name=None):
        self._connections = None
        self._content_url = None
        self._created_at = None
        self._datasource_type = None
        self._id = None
        self._initial_tags = set()
        self._project_name = None
        self._updated_at = None
        self._certified = None
        self._certification_note = None
        self.name = name
        self.owner_id = None
        self.project_id = project_id
        self.tags = set()

        self._permissions = None

    @property
    def connections(self):
        if self._connections is None:
            error = 'Datasource item must be populated with connections first.'
            raise UnpopulatedPropertyError(error)
        return self._connections()

    @property
    def permissions(self):
        if self._permissions is None:
            error = "Project item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._permissions()

    @property
    def content_url(self):
        return self._content_url

    @property
    def created_at(self):
        return self._created_at

    @property
    def certified(self):
        return self._certified

    @certified.setter
    @property_not_nullable
    @property_is_boolean
    def certified(self, value):
        self._certified = value

    @property
    def certification_note(self):
        return self._certification_note

    @certification_note.setter
    def certification_note(self, value):
        self._certification_note = value

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
    def project_name(self):
        return self._project_name

    @property
    def datasource_type(self):
        return self._datasource_type

    @property
    def updated_at(self):
        return self._updated_at

    def _set_connections(self, connections):
        self._connections = connections

    def _set_permissions(self, permissions):
        self._permissions = permissions

    def _parse_common_elements(self, datasource_xml, ns):
        if not isinstance(datasource_xml, ET.Element):
            datasource_xml = ET.fromstring(datasource_xml).find('.//t:datasource', namespaces=ns)
        if datasource_xml is not None:
            (_, _, _, _, _, updated_at, _, project_id, project_name, owner_id,
             certified, certification_note) = self._parse_element(datasource_xml, ns)
            self._set_values(None, None, None, None, None, updated_at, None, project_id,
                             project_name, owner_id, certified, certification_note)
        return self

    def _set_values(self, id, name, datasource_type, content_url, created_at,
                    updated_at, tags, project_id, project_name, owner_id, certified, certification_note):
        if id is not None:
            self._id = id
        if name:
            self.name = name
        if datasource_type:
            self._datasource_type = datasource_type
        if content_url:
            self._content_url = content_url
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
        if certification_note:
            self.certification_note = certification_note
        self.certified = certified  # Always True/False, not conditional

    @classmethod
    def from_response(cls, resp, ns):
        all_datasource_items = list()
        parsed_response = ET.fromstring(resp)
        all_datasource_xml = parsed_response.findall('.//t:datasource', namespaces=ns)

        for datasource_xml in all_datasource_xml:
            (id_, name, datasource_type, content_url, created_at, updated_at,
             tags, project_id, project_name, owner_id,
             certified, certification_note) = cls._parse_element(datasource_xml, ns)
            datasource_item = cls(project_id)
            datasource_item._set_values(id_, name, datasource_type, content_url, created_at, updated_at,
                                        tags, None, project_name, owner_id, certified, certification_note)
            all_datasource_items.append(datasource_item)
        return all_datasource_items

    @staticmethod
    def _parse_element(datasource_xml, ns):
        id_ = datasource_xml.get('id', None)
        name = datasource_xml.get('name', None)
        datasource_type = datasource_xml.get('type', None)
        content_url = datasource_xml.get('contentUrl', None)
        created_at = parse_datetime(datasource_xml.get('createdAt', None))
        updated_at = parse_datetime(datasource_xml.get('updatedAt', None))
        certification_note = datasource_xml.get('certificationNote', None)
        certified = str(datasource_xml.get('isCertified', None)).lower() == 'true'

        tags = None
        tags_elem = datasource_xml.find('.//t:tags', namespaces=ns)
        if tags_elem is not None:
            tags = TagItem.from_xml_element(tags_elem, ns)

        project_id = None
        project_name = None
        project_elem = datasource_xml.find('.//t:project', namespaces=ns)
        if project_elem is not None:
            project_id = project_elem.get('id', None)
            project_name = project_elem.get('name', None)

        owner_id = None
        owner_elem = datasource_xml.find('.//t:owner', namespaces=ns)
        if owner_elem is not None:
            owner_id = owner_elem.get('id', None)

        return (id_, name, datasource_type, content_url, created_at, updated_at, tags, project_id,
                project_name, owner_id, certified, certification_note)
