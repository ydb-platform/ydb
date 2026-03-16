from defusedxml.ElementTree import fromstring

from .property_decorators import property_not_empty


class ColumnItem:
    def __init__(self, name, description=None):
        self._id = None
        self.description = description
        self.name = name

    def __repr__(self):
        return f"<{self.__class__.__name__} {self._id} {self.name} {self.description}>"

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
    def description(self):
        return self._description

    @description.setter
    def description(self, value):
        self._description = value

    @property
    def remote_type(self):
        return self._remote_type

    def _set_values(self, id, name, description, remote_type):
        if id is not None:
            self._id = id
        if name:
            self._name = name
        if description:
            self.description = description
        if remote_type:
            self._remote_type = remote_type

    @classmethod
    def from_response(cls, resp, ns):
        all_column_items = list()
        parsed_response = fromstring(resp)
        all_column_xml = parsed_response.findall(".//t:column", namespaces=ns)

        for column_xml in all_column_xml:
            (id, name, description, remote_type) = cls._parse_element(column_xml, ns)
            column_item = cls(name)
            column_item._set_values(id, name, description, remote_type)
            all_column_items.append(column_item)

        return all_column_items

    @staticmethod
    def _parse_element(column_xml, ns):
        id = column_xml.get("id", None)
        name = column_xml.get("name", None)
        description = column_xml.get("description", None)
        remote_type = column_xml.get("remoteType", None)

        return id, name, description, remote_type
