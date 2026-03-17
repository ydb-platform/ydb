from typing import Callable, Optional, TYPE_CHECKING
from defusedxml.ElementTree import fromstring

from .exceptions import UnpopulatedPropertyError
from .property_decorators import property_not_empty, property_is_boolean

if TYPE_CHECKING:
    from tableauserverclient.models import DQWItem


class TableItem:
    def __init__(self, name, description=None):
        self._id = None
        self.description = description
        self.name = name

        self._contact_id = None
        self._certified = None
        self._certification_note = None
        self._permissions = None
        self._schema = None

        self._columns = None
        self._data_quality_warnings = None

    def __str__(self):
        return f"<{self.__class__.__name__} {self._id} {self._name} >"

    def __repr__(self):
        return self.__str__() + "  { " + ", ".join(" % s: % s" % item for item in vars(self).items()) + "}"

    @property
    def permissions(self):
        if self._permissions is None:
            error = "Project item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._permissions()

    @property
    def dqws(self):
        if self._data_quality_warnings is None:
            error = "Project item must be populated with dqws first."
            raise UnpopulatedPropertyError(error)
        return self._data_quality_warnings()

    @property
    def id(self) -> Optional[str]:
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
    def certified(self):
        return self._certified

    @certified.setter
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
    def contact_id(self):
        return self._contact_id

    @contact_id.setter
    def contact_id(self, value):
        self._contact_id = value

    @property
    def schema(self):
        return self._schema

    @property
    def columns(self):
        if self._columns is None:
            error = "Table must be populated with columns first."
            raise UnpopulatedPropertyError(error)
        #  Each call to `.columns` should create a new pager, this just runs the callable
        return self._columns()

    def _set_columns(self, columns):
        self._columns = columns

    def _set_data_quality_warnings(self, dqw: Callable[[], list["DQWItem"]]) -> None:
        self._data_quality_warnings = dqw

    def _set_values(self, table_values):
        if "id" in table_values:
            self._id = table_values["id"]

        if "name" in table_values:
            self._name = table_values["name"]

        if "description" in table_values:
            self._description = table_values["description"]

        if "isCertified" in table_values:
            self._certified = string_to_bool(table_values["isCertified"])

        if "certificationNote" in table_values:
            self._certification_note = table_values["certificationNote"]

        if "isEmbedded" in table_values:
            self._embedded = string_to_bool(table_values["isEmbedded"])

        if "schema" in table_values:
            self._schema = table_values["schema"]

        if "contact" in table_values:
            self._contact_id = table_values["contact"]["id"]

    def _set_permissions(self, permissions):
        self._permissions = permissions

    @classmethod
    def from_response(cls, resp, ns):
        all_table_items = list()
        parsed_response = fromstring(resp)
        all_table_xml = parsed_response.findall(".//t:table", namespaces=ns)

        for table_xml in all_table_xml:
            parsed_table = cls._parse_element(table_xml, ns)
            table_item = cls(parsed_table["name"])
            table_item._set_values(parsed_table)
            all_table_items.append(table_item)
        return all_table_items

    @staticmethod
    def _parse_element(table_xml, ns):
        table_values = table_xml.attrib.copy()

        contact = table_xml.find(".//t:contact", namespaces=ns)
        if contact is not None:
            table_values["contact"] = contact.attrib.copy()

        return table_values


# Used to convert string represented boolean to a boolean type
def string_to_bool(s):
    return s.lower() == "true"
