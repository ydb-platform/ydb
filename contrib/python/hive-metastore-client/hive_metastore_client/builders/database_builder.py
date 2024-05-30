"""DatabaseBuilder."""
from typing import Mapping

from hive_metastore_client.builders.abstract_builder import AbstractBuilder
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import Database, PrincipalPrivilegeSet, PrincipalType  # type: ignore # noqa: E501


class DatabaseBuilder(AbstractBuilder):
    """Builds thrift Database object."""

    def __init__(
        self,
        name: str,
        description: str = None,
        location_uri: str = None,
        parameters: Mapping[str, str] = None,
        privileges: PrincipalPrivilegeSet = None,
        owner_name: str = None,
        owner_type: PrincipalType = None,
        catalog_name: str = None,
    ):
        """
        Constructor.

        :param name: name of the database
        :param description: description of the database
        :param location_uri: location for the database
        :param parameters: properties associated with the database
        :param privileges: privilege grant info for the database
        :param owner_name: owner name for the database
        :param owner_type: owner type for the database
        :param catalog_name: catalog name for the database
        """
        self.name = name
        self.description = description
        self.location_uri = location_uri
        self.parameters = parameters
        self.privileges = privileges
        self.owner_name = owner_name
        self.owner_type = owner_type
        self.catalog_name = catalog_name

    def build(self) -> Database:
        """Returns the thrift Database object."""
        database = Database(
            name=self.name,
            description=self.description,
            locationUri=self.location_uri,
            parameters=self.parameters,
            privileges=self.privileges,
            ownerName=self.owner_name,
            ownerType=self.owner_type,
            catalogName=self.catalog_name,
        )
        return database
