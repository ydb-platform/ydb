import warnings
from typing import Any

from sqllineage.config import SQLLineageConfig
from sqllineage.exceptions import SQLLineageException
from sqllineage.utils.helpers import escape_identifier_name


class Schema:
    """
    Data Class for Schema
    """

    unknown = "<default>"

    def __init__(self, name: str | None = None):
        """
        :param name: schema name
        """
        if name:
            self.raw_name = escape_identifier_name(name)
        elif SQLLineageConfig.DEFAULT_SCHEMA:
            self.raw_name = escape_identifier_name(SQLLineageConfig.DEFAULT_SCHEMA)
        else:
            self.raw_name = escape_identifier_name(Schema.unknown)

    def __str__(self):
        return self.raw_name

    def __repr__(self):
        return "Schema: " + str(self)

    def __eq__(self, other):
        return isinstance(other, Schema) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    def __bool__(self):
        return str(self) != self.unknown


class Table:
    """
    Data Class for Table
    """

    def __init__(self, name: str, schema: Schema = Schema(), **kwargs):
        """
        :param name: table name
        :param schema: schema as defined by :class:`Schema`
        """
        if "." not in name:
            self.schema = schema
            self.raw_name = escape_identifier_name(name)
        else:
            schema_name, table_name = name.rsplit(".", 1)
            if len(schema_name.split(".")) > 2:
                # allow db.schema as schema_name, but a.b.c as schema_name is forbidden
                raise SQLLineageException("Invalid format for table name: %s.", name)
            self.schema = Schema(schema_name)
            self.raw_name = escape_identifier_name(table_name)
            if schema:
                warnings.warn("Name is in schema.table format, schema param is ignored")
        self.alias = escape_identifier_name(kwargs.pop("alias", self.raw_name))

    def __str__(self):
        return f"{self.schema}.{self.raw_name}"

    def __repr__(self):
        return "Table: " + str(self)

    def __eq__(self, other):
        return isinstance(other, Table) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    @staticmethod
    def of(table: Any) -> "Table":
        raise NotImplementedError


class Path:
    """
    Data Class for Path
    """

    def __init__(self, uri: str):
        """
        :param uri: uri of the path
        """
        self.uri = escape_identifier_name(uri)

    def __str__(self):
        return self.uri

    def __repr__(self):
        return "Path: " + str(self)

    def __eq__(self, other):
        return isinstance(other, Path) and self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)


class SubQuery:
    """
    Data Class for SubQuery
    """

    def __init__(self, subquery: Any, subquery_raw: str, alias: str | None):
        """
        :param subquery: subquery
        :param alias: subquery alias name
        """
        self.query = subquery
        self.query_raw = subquery_raw
        self.alias = (
            escape_identifier_name(alias)
            if alias is not None
            else f"subquery_{hash(self)}"
        )

    def __str__(self):
        return self.alias

    def __repr__(self):
        return "SubQuery: " + str(self)

    def __eq__(self, other):
        return isinstance(other, SubQuery) and self.query_raw == other.query_raw

    def __hash__(self):
        return hash(self.query_raw)

    @staticmethod
    def of(subquery: Any, alias: str | None) -> "SubQuery":
        raise NotImplementedError


class Column:
    """
    Data Class for Column
    """

    def __init__(self, name: str, **kwargs):
        """
        :param name: column name
        :param parent: :class:`Table` or :class:`SubQuery`
        :param kwargs:
        """
        self._parent: set[Path | Table | SubQuery] = set()
        self.raw_name = escape_identifier_name(name)
        self.source_columns = [
            (
                escape_identifier_name(raw_name),
                escape_identifier_name(qualifier) if qualifier is not None else None,
            )
            for raw_name, qualifier in kwargs.pop(
                "source_columns", ((self.raw_name, None),)
            )
        ]
        self.from_alias = kwargs.pop("from_alias", False)

    def __str__(self):
        return (
            f"{self.parent}.{self.raw_name}"
            if self.parent is not None and not isinstance(self.parent, Path)
            else f"{self.raw_name}"
        )

    def __repr__(self):
        return "Column: " + str(self)

    def __eq__(self, other):
        return (
            isinstance(other, Column)
            and str(self) == str(other)
            and self.parent == other.parent
        )

    def __hash__(self):
        return hash(str(self))

    @property
    def parent(self) -> Path | Table | SubQuery | None:
        return next(iter(self._parent)) if len(self._parent) == 1 else None

    @parent.setter
    def parent(self, value: Path | Table | SubQuery):
        self._parent.add(value)

    @property
    def parent_candidates(self) -> list[Path | Table | SubQuery]:
        return sorted(self._parent, key=lambda p: str(p))

    @staticmethod
    def of(column: Any, **kwargs) -> "Column":
        """
        Build a 'Column' object
        :param column: column segment or token
        :return:
        """
        raise NotImplementedError

    def to_source_columns(self, alias_mapping: dict[str, Path | Table | SubQuery]):
        """
        Best guess for source table given all the possible table/subquery and their alias.
        """

        def _to_src_col(
            name: str, parent: Path | Table | SubQuery | None = None
        ) -> Column:
            col = Column(name)
            if parent:
                col.parent = parent
            return col

        source_columns = set()
        for src_col, qualifier in self.source_columns:
            if qualifier is None:
                if src_col == "*":
                    # select *
                    for table in set(alias_mapping.values()):
                        source_columns.add(_to_src_col(src_col, table))
                else:
                    # select unqualified column
                    source = _to_src_col(src_col, None)
                    for table in set(alias_mapping.values()):
                        # in case of only one table, we get the right answer
                        # in case of multiple tables, a bunch of possible tables are set
                        source.parent = table
                    source_columns.add(source)
            else:
                if alias_mapping.get(qualifier):
                    source_columns.add(
                        _to_src_col(src_col, alias_mapping.get(qualifier))
                    )
                else:
                    source_columns.add(_to_src_col(src_col, Table(qualifier)))
        return source_columns
