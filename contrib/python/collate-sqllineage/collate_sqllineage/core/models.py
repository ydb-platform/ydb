import logging
import warnings
from typing import Any, Dict, List, Optional, Set, Union

from collate_sqllineage.exceptions import SQLLineageException
from collate_sqllineage.utils.helpers import escape_identifier_name

logger = logging.getLogger(__name__)


class Schema:
    """
    Data Class for Schema
    """

    unknown = "<default>"

    def __init__(self, name: str = unknown):
        """
        :param name: schema name
        """
        self._str = None
        self._hash = None
        self.raw_name = escape_identifier_name(name)

    def __str__(self):
        if self._str is None:
            self._str = self.raw_name.lower()
        return self._str

    def __repr__(self):
        return "Schema: " + str(self)

    def __eq__(self, other):
        return isinstance(other, Schema) and str(self) == str(other)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(str(self))
        return self._hash

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
        self._str = None
        self._hash = None
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
        self.alias = kwargs.pop("alias", self.raw_name)

    def __str__(self):
        if self._str is None:
            self._str = f"{self.schema}.{self.raw_name.lower()}"
        return self._str

    def __repr__(self):
        return "Table: " + str(self)

    def __eq__(self, other):
        return isinstance(other, Table) and str(self) == str(other)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(str(self))
        return self._hash

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


class Location:
    """
    Data Class for Location (named database object referencing storage).
    """

    def __init__(self, name: str, schema: Schema = Schema(), **kwargs):
        """
        :param name: location name (e.g. @STAGE_01, @DB.SCHEMA.STAGE)
        :param schema: schema as defined by :class:`Schema`
        """
        self._str = None
        self._hash = None
        # Strip leading @ for internal representation
        raw = name.lstrip("@")
        if "." not in raw:
            self.schema = schema
            self.raw_name = escape_identifier_name(raw)
        else:
            schema_name, loc_name = raw.rsplit(".", 1)
            if len(schema_name.split(".")) > 2:
                raise SQLLineageException("Invalid format for location name: %s.", name)
            self.schema = Schema(schema_name)
            self.raw_name = escape_identifier_name(loc_name)
            if schema:
                warnings.warn(
                    "Name is in schema.location format, schema param is ignored"
                )
        self.alias = kwargs.pop("alias", self.raw_name)

    def __str__(self):
        if self._str is None:
            self._str = f"{self.schema}.{self.raw_name.lower()}"
        return self._str

    def __repr__(self):
        return "Location: " + str(self)

    def __eq__(self, other):
        return isinstance(other, Location) and str(self) == str(other)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(str(self))
        return self._hash


class DataFunction:
    """
    Data Class for Function
    """

    def __init__(self, name: str, schema: Schema = Schema(), **kwargs):
        """
        :param name: function name
        :param schema: schema as defined by :class:`Schema`
        """
        self._str = None
        self._hash = None
        if "." not in name:
            self.schema = schema
            self.raw_name = escape_identifier_name(name)
        else:
            schema_name, function_name = name.rsplit(".", 1)
            if len(schema_name.split(".")) > 2:
                # allow db.schema as schema_name, but a.b.c as schema_name is forbidden
                raise SQLLineageException("Invalid format for function name: %s.", name)
            self.schema = Schema(schema_name)
            self.raw_name = escape_identifier_name(function_name)
            if schema:
                warnings.warn(
                    "Name is in schema.function format, schema param is ignored"
                )
        self.alias = kwargs.pop("alias", self.raw_name)

    def __str__(self):
        if self._str is None:
            self._str = f"{self.schema}.{self.raw_name.lower()}"
        return self._str

    def __repr__(self):
        return "DataFunction: " + str(self)

    def __eq__(self, other):
        return isinstance(other, DataFunction) and str(self) == str(other)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(str(self))
        return self._hash

    @staticmethod
    def of(function: Any) -> "DataFunction":
        raise NotImplementedError


class SubQuery:
    """
    Data Class for SubQuery
    """

    def __init__(self, subquery: Any, subquery_raw: str, alias: Optional[str]):
        """
        :param subquery: subquery
        :param alias: subquery alias name
        """
        self.query = subquery
        self.query_raw = subquery_raw
        self.alias = alias if alias is not None else f"subquery_{hash(self)}"

    def __str__(self):
        return self.alias

    def __repr__(self):
        return "SubQuery: " + str(self)

    def __eq__(self, other):
        return isinstance(other, SubQuery) and self.query_raw == other.query_raw

    def __hash__(self):
        return hash(self.query_raw)

    @staticmethod
    def of(subquery: Any, alias: Optional[str]) -> "SubQuery":
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
        self._str = None
        self._final_parent: Optional[
            Union[DataFunction, Location, Path, Table, SubQuery]
        ] = None
        self._hash = None
        self._parent: Set[Union[DataFunction, Location, Path, Table, SubQuery]] = set()
        self.raw_name = escape_identifier_name(name)
        self.source_columns = kwargs.pop("source_columns", ((self.raw_name, None),))

    def __str__(self):
        if self._str is None:
            self._str = (
                f"{self.parent}.{self.raw_name.lower()}"
                if self.parent is not None and not isinstance(self.parent, Path)
                else f"{self.raw_name.lower()}"
            )
        return self._str

    def __repr__(self):
        return "Column: " + str(self)

    def __eq__(self, other):
        return isinstance(other, Column) and str(self) == str(other)

    def __hash__(self):
        if self._hash is None:
            self._hash = hash(str(self))
        return self._hash

    @property
    def parent(
        self,
    ) -> Optional[Union[DataFunction, Location, Path, Table, SubQuery]]:
        if self._final_parent is None:
            self._final_parent = (
                list(self._parent)[0] if len(self._parent) == 1 else None
            )
        return self._final_parent

    @parent.setter
    def parent(self, value: Union[DataFunction, Location, Path, Table, SubQuery]):
        self._parent.add(value)

    @property
    def parent_candidates(
        self,
    ) -> List[Union[DataFunction, Location, Path, Table, SubQuery]]:
        return sorted(self._parent, key=lambda p: str(p))

    @staticmethod
    def of(column: Any, **kwargs) -> "Column":
        """
        Build a 'Column' object
        :param column: column segment or token
        :return:
        """
        raise NotImplementedError

    def to_source_columns(
        self,
        alias_mapping: Dict[str, Union[DataFunction, Location, Path, Table, SubQuery]],
    ):
        """
        Best guess for source table given all the possible table/subquery and their alias.
        """

        def _to_src_col(
            name: str,
            parent: Optional[
                Union[DataFunction, Location, Path, Table, SubQuery]
            ] = None,
        ):
            col = Column(name)
            if parent:
                col.parent = parent
            return col

        source_columns = set()
        for src_col, qualifier in self.source_columns:
            try:
                if qualifier is None:
                    if src_col == "*":
                        # select *
                        for table in set(alias_mapping.values()):
                            source_columns.add(_to_src_col(src_col, table))
                    else:
                        # select unqualified column
                        src_col = _to_src_col(src_col, None)
                        for table in set(alias_mapping.values()):
                            # in case of only one table, we get the right answer
                            # in case of multiple tables, a bunch of possible tables are set
                            src_col.parent = table
                        if (
                            src_col is not None
                            and hasattr(src_col, "raw_name")
                            and src_col.raw_name is not None
                        ):
                            source_columns.add(src_col)
                else:
                    # Try case-insensitive lookup for qualifier
                    resolved_table = alias_mapping.get(qualifier) or alias_mapping.get(
                        qualifier.lower()
                    )
                    if resolved_table:
                        source_columns.add(_to_src_col(src_col, resolved_table))
                    else:
                        source_columns.add(_to_src_col(src_col, Table(qualifier)))
            except Exception as ex:
                logger.error(f"Error processing column {self.raw_name}: {ex}")
        return source_columns


class AnalyzerContext:
    """
    Data class to hold the analyzer context
    """

    def __init__(
        self,
        subquery: Optional[SubQuery] = None,
        prev_cte: Optional[Set[SubQuery]] = None,
        prev_write: Optional[Set[Union[SubQuery, Table]]] = None,
        target_columns=None,
    ):
        """
        :param subquery: subquery
        :param prev_cte: previous CTE queries
        :param prev_write: previous written tables
        :param target_columns: previous target columns
        """
        if target_columns is None:
            target_columns = []
        self.subquery = subquery
        self.prev_cte = prev_cte
        self.prev_write = prev_write
        self.target_columns = target_columns
