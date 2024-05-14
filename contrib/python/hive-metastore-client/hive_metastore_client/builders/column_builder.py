"""ColumnBuilder."""
from hive_metastore_client.builders.abstract_builder import AbstractBuilder
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import FieldSchema  # type: ignore # noqa: E501


class ColumnBuilder(AbstractBuilder):
    """Builds thrift FieldSchema object."""

    def __init__(self, name: str, type: str, comment: str = None) -> None:
        """
        Constructor.

        :param name: name of the field
        :param type: type of the field
        :param comment: column's comment
        """
        self.name = name
        self.type = type
        self.comment = comment

    def build(self) -> FieldSchema:
        """Returns the thrift FieldSchema object."""
        return FieldSchema(name=self.name, type=self.type, comment=self.comment)
