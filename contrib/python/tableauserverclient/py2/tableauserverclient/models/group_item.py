import xml.etree.ElementTree as ET
from .exceptions import UnpopulatedPropertyError
from .property_decorators import property_not_empty
from .reference_item import ResourceReference


class GroupItem(object):

    tag_name = 'group'

    def __init__(self, name=None):
        self._domain_name = None
        self._id = None
        self._users = None
        self.name = name

    @property
    def domain_name(self):
        return self._domain_name

    @property
    def id(self):
        return self._id

    @property
    def name(self):
        return self._name

    @name.setter
    @property_not_empty
    def name(self, value):
        self._name = value

    @property
    def users(self):
        if self._users is None:
            error = "Group must be populated with users first."
            raise UnpopulatedPropertyError(error)
        #  Each call to `.users` should create a new pager, this just runs the callable
        return self._users()

    def to_reference(self):
        return ResourceReference(id_=self.id, tag_name=self.tag_name)

    def _set_users(self, users):
        self._users = users

    @classmethod
    def from_response(cls, resp, ns):
        all_group_items = list()
        parsed_response = ET.fromstring(resp)
        all_group_xml = parsed_response.findall('.//t:group', namespaces=ns)
        for group_xml in all_group_xml:
            name = group_xml.get('name', None)
            group_item = cls(name)
            group_item._id = group_xml.get('id', None)

            domain_elem = group_xml.find('.//t:domain', namespaces=ns)
            if domain_elem is not None:
                group_item._domain_name = domain_elem.get('name', None)
            all_group_items.append(group_item)
        return all_group_items

    @staticmethod
    def as_reference(id_):
        return ResourceReference(id_, GroupItem.tag_name)
