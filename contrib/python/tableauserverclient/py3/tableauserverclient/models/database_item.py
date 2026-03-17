import logging

from defusedxml.ElementTree import fromstring

from .exceptions import UnpopulatedPropertyError
from .property_decorators import (
    property_is_enum,
    property_not_empty,
    property_is_boolean,
)


class DatabaseItem:
    class ContentPermissions:
        LockedToProject = "LockedToDatabase"
        ManagedByOwner = "ManagedByOwner"

    def __init__(self, name, description=None, content_permissions=None):
        self._id = None
        self.name = name
        self.description = description
        self.content_permissions = content_permissions
        self._certified = None
        self._certification_note = None
        self._contact_id = None

        self._connector_url = None
        self._connection_type = None
        self._embedded = None
        self._file_extension = None
        self._file_id = None
        self._file_path = None
        self._host_name = None
        self._metadata_type = None
        self._mime_type = None
        self._port = None
        self._provider = None
        self._request_url = None

        self._permissions = None
        self._default_table_permissions = None

        self._data_quality_warnings = None

        self._tables = None  # Not implemented yet

    def __str__(self):
        return f"<Database {self._id} '{self.name}'>"

    def __repr__(self):
        return self.__str__() + "  { " + ", ".join(" % s: % s" % item for item in vars(self).items()) + "}"

    @property
    def dqws(self):
        if self._data_quality_warnings is None:
            error = "Project item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._data_quality_warnings()

    @property
    def content_permissions(self):
        return self._content_permissions

    @content_permissions.setter
    @property_is_enum(ContentPermissions)
    def content_permissions(self, value):
        self._content_permissions = value

    @property
    def permissions(self):
        if self._permissions is None:
            error = "Project item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._permissions()

    @property
    def default_table_permissions(self):
        if self._default_table_permissions is None:
            error = "Project item must be populated with permissions first."
            raise UnpopulatedPropertyError(error)
        return self._default_table_permissions()

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
    def embedded(self):
        return self._embedded

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
    def metadata_type(self):
        return self._metadata_type

    @property
    def host_name(self):
        return self._host_name

    @property
    def port(self):
        return self._port

    @property
    def file_path(self):
        return self._file_path

    @property
    def provider(self):
        return self._provider

    @property
    def mime_type(self):
        return self._mime_type

    @property
    def connector_url(self):
        return self._connector_url

    @property
    def connection_type(self):
        return self._connection_type

    @property
    def request_url(self):
        return self._request_url

    @property
    def file_extension(self):
        return self._file_extension

    @property
    def file_id(self):
        return self._file_id

    @property
    def contact_id(self):
        return self._contact_id

    @contact_id.setter
    def contact_id(self, value):
        self._contact_id = value

    @property
    def tables(self):
        if self._tables is None:
            error = "Database must be populated with tables first."
            raise UnpopulatedPropertyError(error)
        #  Each call to `.tables` should create a new pager, this just runs the callable
        return self._tables()

    def _set_values(self, database_values):
        # ID & Settable
        if "id" in database_values:
            self._id = database_values["id"]

        if "contact" in database_values:
            self._contact_id = database_values["contact"]["id"]

        if "name" in database_values:
            self._name = database_values["name"]

        if "description" in database_values:
            self._description = database_values["description"]

        if "isCertified" in database_values:
            self._certified = string_to_bool(database_values["isCertified"])

        if "certificationNote" in database_values:
            self._certification_note = database_values["certificationNote"]

        # Not settable, alphabetical

        if "connectionType" in database_values:
            self._connection_type = database_values["connectionType"]

        if "connectorUrl" in database_values:
            self._connector_url = database_values["connectorUrl"]

        if "contentPermissions" in database_values:
            self._content_permissions = database_values["contentPermissions"]

        if "isEmbedded" in database_values:
            self._embedded = string_to_bool(database_values["isEmbedded"])

        if "fileExtension" in database_values:
            self._file_extension = database_values["fileExtension"]

        if "fileId" in database_values:
            self._file_id = database_values["fileId"]

        if "filePath" in database_values:
            self._file_path = database_values["filePath"]

        if "hostName" in database_values:
            self._host_name = database_values["hostName"]

        if "mimeType" in database_values:
            self._mime_type = database_values["mimeType"]

        if "port" in database_values:
            self._port = int(database_values["port"])

        if "provider" in database_values:
            self._provider = database_values["provider"]

        if "requestUrl" in database_values:
            self._request_url = database_values["requestUrl"]

        if "type" in database_values:
            self._metadata_type = database_values["type"]

    def _set_permissions(self, permissions):
        self._permissions = permissions

    def _set_tables(self, tables):
        self._tables = tables

    def _set_default_permissions(self, permissions, content_type):
        attr = f"_default_{content_type}_permissions"
        setattr(
            self,
            attr,
            permissions,
        )
        logging.getLogger().debug({"type": attr, "value": getattr(self, attr)})

    def _set_data_quality_warnings(self, dqw):
        self._data_quality_warnings = dqw

    @classmethod
    def from_response(cls, resp, ns):
        all_database_items = list()
        parsed_response = fromstring(resp)
        all_database_xml = parsed_response.findall(".//t:database", namespaces=ns)

        for database_xml in all_database_xml:
            parsed_database = cls._parse_element(database_xml, ns)
            database_item = cls(parsed_database["name"])
            database_item._set_values(parsed_database)
            all_database_items.append(database_item)
        return all_database_items

    @staticmethod
    def _parse_element(database_xml, ns):
        database_values = database_xml.attrib.copy()
        contact = database_xml.find(".//t:contact", namespaces=ns)
        if contact is not None:
            database_values["contact"] = contact.attrib.copy()
        return database_values


# Used to convert string represented boolean to a boolean type
def string_to_bool(s):
    return s.lower() == "true"
