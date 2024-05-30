"""Hive Metastore Client main class."""
import copy
from typing import List, Any, Tuple

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TTransport

from thrift_files.libraries.thrift_hive_metastore_client.ThriftHiveMetastore import (  # type: ignore # noqa: E501
    Client as ThriftClient,
)
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (  # type: ignore # noqa: E501
    StorageDescriptor,
    Partition,
    FieldSchema,
    Database,
    AlreadyExistsException,
    Table,
    PartitionValuesRequest,
    NoSuchObjectException,
)


class HiveMetastoreClient(ThriftClient):
    """User main interface with the metastore server methods."""

    COL_TYPE_INCOMPATIBILITY_DISALLOW_CONFIG = (
        "hive.metastore.disallow.incompatible.col.type.changes"
    )

    def __init__(self, host: str, port: int = 9083) -> None:
        """
        Instantiates the client object for given host and port.

        :param host: thrive metastore host. I.g.: https://xpto.com
        :param port: hive metastore port. Default is 9083.
        """
        protocol = self._init_protocol(host, port)
        super().__init__(protocol)

    @staticmethod
    def _init_protocol(host: str, port: int) -> TBinaryProtocol:
        """
        Instantiates the binary protocol object.

        This object contains the implementation of the Thrift protocol driver.

        :param host: thrive metastore host. I.g.: https://xpto.com
        :param port: the hive metastore port
        :return: the Thrift protocol driver
        :rtype: thrift.protocol.TBinaryProtocol.TBinaryProtocol
        """
        transport = TSocket.TSocket(host, int(port))
        transport = TTransport.TBufferedTransport(transport)

        return TBinaryProtocol.TBinaryProtocol(transport)

    def open(self) -> "HiveMetastoreClient":
        """
        Opens the connection with the Thrift server.

        :return: HiveMetastoreClientConnector instance
        """
        self._oprot.trans.open()
        return self

    def close(self) -> None:
        """Closes the connection with the Thrift server."""
        self._oprot.trans.close()

    def __enter__(self) -> "HiveMetastoreClient":
        """Handles the conn opening whenever the 'with' block statement is used."""
        self.open()
        return self

    def __exit__(self, exc_type: str, exc_val: str, exc_tb: str) -> None:
        """Handles the conn closing after the code inside 'with' block is ended."""
        self.close()

    def add_columns_to_table(
        self, db_name: str, table_name: str, columns: List[FieldSchema]
    ) -> None:
        """
        Adds columns to a table.

        :param db_name: database name of the table
        :param table_name: table name
        :param columns: columns to be added to the table
        """
        table = self.get_table(dbname=db_name, tbl_name=table_name)

        # add more columns to the list of columns
        table.sd.cols.extend(columns)

        # call alter table to add columns
        self.alter_table(dbname=db_name, tbl_name=table_name, new_tbl=table)

    def drop_columns_from_table(
        self, db_name: str, table_name: str, columns: List[str]
    ) -> None:
        """
        Drops columns from a table.

        It encapsulates the logic of calling alter table with removed columns from
        the list of columns, since hive does not have a drop command.

        :param db_name: database name of the table
        :param table_name: table name
        :param columns: names of the columns to be dropped from the table
        """
        if columns:
            table = self.get_table(dbname=db_name, tbl_name=table_name)

            # remove columns from the list of columns in table object
            cols = []
            for col in table.sd.cols:
                if col.name not in columns:
                    cols.append(col)
            table.sd.cols = cols

            # Hive Metastore enforces that the schema prior and after
            # an ALTER TABLE should be the same,
            # however when dropping a column the schema will definitely change
            self.setMetaConf(self.COL_TYPE_INCOMPATIBILITY_DISALLOW_CONFIG, "false")

            # call alter table to drop columns removed from list of table columns
            self.alter_table(dbname=db_name, tbl_name=table_name, new_tbl=table)

    def add_partitions_if_not_exists(
        self, db_name: str, table_name: str, partition_list: List[Partition]
    ) -> None:
        """
        Add partitions to a table if it does not exist.

        If a partition is added twice, the method handles the
         AlreadyExistsException, not raising the exception.

        :param db_name: database name where the table is at
        :param table_name: table name which the partitions belong to
        :param partition_list: list of partitions to be added to the table
        """
        if not partition_list:
            raise ValueError(
                "m=add_partitions_if_not_exists, msg=The partition list is empty."
            )

        table = self.get_table(dbname=db_name, tbl_name=table_name)

        partition_list_with_correct_location = self._format_partitions_location(
            partition_list=partition_list,
            table_storage_descriptor=table.sd,
            table_partition_keys=table.partitionKeys,
        )

        for partition in partition_list_with_correct_location:
            try:
                self.add_partition(partition)
            except AlreadyExistsException:
                pass

    def add_partitions_to_table(
        self, db_name: str, table_name: str, partition_list: List[Partition]
    ) -> None:
        """
        Add partitions to a table.

        If any partition of partition_list already exists, an
         AlreadyExistsException, will be thrown and no partition
         will be added.

        :param db_name: database name where the table is at
        :param table_name: table name which the partitions belong to
        :param partition_list: list of partitions to be added to the table
        """
        if not partition_list:
            raise ValueError(
                "m=add_partitions_if_not_exists, msg=The partition list is empty."
            )

        table = self.get_table(dbname=db_name, tbl_name=table_name)

        partition_list_with_correct_location = self._format_partitions_location(
            partition_list=partition_list,
            table_storage_descriptor=table.sd,
            table_partition_keys=table.partitionKeys,
        )

        self.add_partitions(partition_list_with_correct_location)

    def create_database_if_not_exists(self, database: Database) -> None:
        """
        Creates the table in Hive Metastore if it does not exist.

        Since hive metastore server and thrift mapping do not have the option
         of checking if the database does not exist, this method simulates this
         this behavior.

        :param database: the database object
        """
        try:
            self.create_database(database)
        except AlreadyExistsException:
            pass

    def create_external_table(self, table: Table) -> None:
        """
        Creates an external table in Hive Metastore.

        When a table is created with tableType default (None) or equal to
         EXTERNAL_TABLE there is an error in the server that creates the table
         as a MANAGED_TABLE.
        This method enforces the parameter EXTERNAL=TRUE so the table is
         created correctly.

        :param table: the table object
        """
        table.parameters = {"EXTERNAL": "TRUE"}
        table.tableType = "EXTERNAL_TABLE"
        self.create_table(table)

    @staticmethod
    def _format_partitions_location(
        partition_list: List[Partition],
        table_storage_descriptor: StorageDescriptor,
        table_partition_keys: List[FieldSchema],
    ) -> List[Partition]:
        """
        Format the location of partitions, adding a specific value to each object.

        It is based on the location of the Table plus the provided
        individual values per Partition.

        :param partition_list: list of partitions
        :param table_storage_descriptor: the object StorageDescriptor related
        to the Table
        :param table_partition_keys: list of columns that are defined as the
        Table partitions
        :return: list of partitions with the correct location
        """
        # identify partitions key from table definition
        partition_keys = []
        for key in table_partition_keys:
            partition_keys.append(key.name)

        for partition in partition_list:
            HiveMetastoreClient._validate_lists_length(partition_keys, partition.values)

            # organize keys and values in partition expected format
            location_suffix = [
                partition_name + "=" + value
                for partition_name, value in zip(partition_keys, partition.values)
            ]
            current_storage_descriptor = copy.deepcopy(table_storage_descriptor)
            current_storage_descriptor.location += "/" + "/".join(location_suffix)

            # set the changed storage_descriptor to the current partition
            partition.sd = current_storage_descriptor

        return partition_list

    @staticmethod
    def _validate_lists_length(list_a: List[Any], list_b: List[Any]) -> None:
        """
        Validate if the two list have the same length.

        :param list_a: first list to be compared
        :param list_b: second list to be compared
        """
        if len(list_a) != len(list_b):
            raise ValueError(
                "m=_validate_lists_length, msg=The length of the two provided "
                "lists does not match"
            )

    def get_partition_keys_objects(
        self, db_name: str, table_name: str
    ) -> List[FieldSchema]:
        """
        Gets the partition keys objects, containing the metadata, from a table.

        An empty list will be returned when no table is found or
        when the table has no partitions

        :param db_name: database name where the table is at
        :param table_name: table name which the partition keys belong to
        """
        table = self.get_table(dbname=db_name, tbl_name=table_name)
        return list(table.partitionKeys) if table else []

    def get_partition_keys_names(self, db_name: str, table_name: str) -> List[str]:
        """
        Gets the partition keys names from a table.

        An empty list will be returned when no table is found or
        when the table has no partitions

        :param db_name: database name where the table is at
        :param table_name: table name which the partition keys belong to
        """
        partition_keys = self.get_partition_keys_objects(
            db_name=db_name, table_name=table_name
        )
        return [partition.name for partition in partition_keys]

    def get_partition_keys(
        self, db_name: str, table_name: str
    ) -> List[Tuple[str, str]]:
        """
        Gets the partition keys from a table as a tuple: (name, type).

        An empty list will be returned when no table is found or
        when the table has no partitions.

        :param db_name: database name where the table is at
        :param table_name: table name which the partition keys belong to
        """
        partition_keys = self.get_partition_keys_objects(
            db_name=db_name, table_name=table_name
        )
        return [(partition.name, partition.type) for partition in partition_keys]

    def bulk_drop_partitions(
        self,
        db_name: str,
        table_name: str,
        partition_list: List[List[str]],
        delete_data: bool = False,
    ) -> None:
        """
        Drops the partitions values from the partition list.

        This methods simulates a bulk drop for the user, since the server only
         supports an unitary drop.
        If some partition cannot be dropped an exception will be thrown in the
         end of execution.

        :param db_name: database name of the table
        :param table_name: table name
        :param partition_list: the partitions to be dropped
        :param delete_data: indicates whether the data respective to the
         partition should be dropped in the source.
        :raises: NoSuchObjectException
        """
        partitions_not_dropped = []
        for partition_values in partition_list:
            try:
                self.drop_partition(db_name, table_name, partition_values, delete_data)
            except NoSuchObjectException:
                partitions_not_dropped.append(partition_values)

        if partitions_not_dropped:
            raise NoSuchObjectException(
                "m=bulk_drop_partitions, partitions_not_dropped="
                f"{partitions_not_dropped}, msg=Some partition values were not "
                "dropped because they do not exist."
            )

    def get_partition_values_from_table(
        self, db_name: str, table_name: str
    ) -> List[List[str]]:
        """
        Gets the partition names from a table.

        It automatically fetches the table's partition keys.

        An empty list will be returned when no table is found or
        when the table has no partitions.

        :param db_name: database name where the table is at
        :param table_name: table name which the partitions belong to
        """
        partition_keys = self.get_partition_keys_objects(
            db_name=db_name, table_name=table_name
        )

        partitions = []
        if partition_keys:
            partition_values_response = self.get_partition_values(
                PartitionValuesRequest(
                    dbName=db_name, tblName=table_name, partitionKeys=partition_keys,
                )
            )
            partitions = [
                partition.row for partition in partition_values_response.partitionValues
            ]

        return partitions
