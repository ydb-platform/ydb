"""PartitionBuilder."""
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import Partition, StorageDescriptor, PrincipalPrivilegeSet  # type: ignore # noqa: E501
from typing import List, Mapping


class PartitionBuilder:
    """Builds thrift Partition object."""

    def __init__(
        self,
        values: List[str],
        db_name: str,
        table_name: str,
        sd: StorageDescriptor = None,
        create_time: int = None,
        last_access_time: int = None,
        parameters: Mapping[str, str] = None,
        privileges: PrincipalPrivilegeSet = None,
        cat_name: str = None,
    ) -> None:
        """
        Constructor.

        :param values: list of partition key names
        :param db_name: database name
        :param table_name: table name
        :param create_time: creation time in epoch of the partition
        :param last_access_time: last access time in epoch of the partition
        :param sd: thrift storage descriptor struct object
        :param parameters: to store comments or any other user level parameters
        :param privileges: thrift principal_privilege_set struct object
        :param cat_name: name of the catalog the table is in
        """
        self.values = values
        self.db_name = db_name
        self.table_name = table_name
        self.create_time = create_time
        self.last_access_time = last_access_time
        self.sd = sd
        self.parameters = parameters
        self.privileges = privileges
        self.cat_name = cat_name

    def build(self) -> Partition:
        """Returns the thrift Partition object."""
        return Partition(
            values=self.values,
            dbName=self.db_name,
            tableName=self.table_name,
            createTime=self.create_time,
            lastAccessTime=self.last_access_time,
            sd=self.sd,
            parameters=self.parameters,
            privileges=self.privileges,
            catName=self.cat_name,
        )
