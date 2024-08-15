from dataclasses import dataclass
import datetime
from typing import TypeAlias, List, Any, Optional, Sequence, Dict

from yt import yson
from yt.yson.yson_types import YsonEntity
import ydb.public.api.protos.ydb_value_pb2 as ydb_value
from ydb.library.yql.providers.generic.connector.api.common.data_source_pb2 import EDataSourceKind
from ydb.public.api.protos.ydb_value_pb2 import Type, OptionalType

import ydb.library.yql.providers.generic.connector.tests.utils.types.clickhouse as clickhouse
import ydb.library.yql.providers.generic.connector.tests.utils.types.mysql as mysql
import ydb.library.yql.providers.generic.connector.tests.utils.types.oracle as oracle
import ydb.library.yql.providers.generic.connector.tests.utils.types.postgresql as postgresql
import ydb.library.yql.providers.generic.connector.tests.utils.types.ydb as Ydb

YsonList: TypeAlias = yson.yson_types.YsonList


@dataclass
class DataSourceType:
    ch: clickhouse.Type = None
    my: mysql.Type = None
    ora: oracle.Type = None
    pg: postgresql.Type = None
    ydb: Ydb.Type = None

    def pick(self, kind: EDataSourceKind.ValueType) -> str:
        target = None
        match kind:
            case EDataSourceKind.CLICKHOUSE:
                target = self.ch
            case EDataSourceKind.MYSQL:
                target = self.my
            case EDataSourceKind.ORACLE:
                target = self.ora
            case EDataSourceKind.POSTGRESQL:
                target = self.pg
            case EDataSourceKind.YDB:
                target = self.ydb
            case _:
                raise Exception(f'invalid data source: {kind}')

        return target.to_sql()


@dataclass
class Column:
    name: str
    ydb_type: ydb_value.Type
    data_source_type: DataSourceType

    def __eq__(self, __value: object) -> bool:
        if isinstance(__value, self.__class__):
            return self.name == __value.name and self.ydb_type == __value.ydb_type
        else:
            raise Exception(f"can't compare 'Column' with '{__value.__class__}'")

    @classmethod
    def from_yson(cls, src: YsonList):
        name = src[0]
        type_info = src[1]
        assert isinstance(type_info, list), type_info

        ydb_type = None

        if type_info[0] == 'OptionalType':
            primitive_type = ydb_value.Type(type_id=cls.__parse_primitive_type(type_info[1][1]))
            ydb_type = ydb_value.Type(optional_type=ydb_value.OptionalType(item=primitive_type))
        else:
            ydb_type = ydb_value.Type(type_id=cls.__parse_primitive_type(type_info[1]))

        return cls(name=name, ydb_type=ydb_type, data_source_type=None)

    @classmethod
    def from_json(cls, src: Dict):
        name = src["name"]

        if "optional_type" in src["type"]:
            primitive_type = ydb_value.Type(type_id=src["type"]["optional_type"]["item"]["type_id"])
            ydb_type = ydb_value.Type(optional_type=ydb_value.OptionalType(item=primitive_type))
        else:
            ydb_type = ydb_value.Type(type_id=src["type"]["type_id"])

        return cls(name=name, ydb_type=ydb_type, data_source_type=None)

    @staticmethod
    def __parse_primitive_type(src: str) -> ydb_value.Type.PrimitiveTypeId:
        match src:
            case "Bool":
                return ydb_value.Type.BOOL
            case "Utf8":
                return ydb_value.Type.UTF8
            case "Json":
                return ydb_value.Type.JSON
            case "String":
                return ydb_value.Type.STRING
            case "Int8":
                return ydb_value.Type.INT8
            case "Int16":
                return ydb_value.Type.INT16
            case "Int32":
                return ydb_value.Type.INT32
            case "Int64":
                return ydb_value.Type.INT64
            case "Uint8":
                return ydb_value.Type.UINT8
            case "Uint16":
                return ydb_value.Type.UINT16
            case "Uint32":
                return ydb_value.Type.UINT32
            case "Uint64":
                return ydb_value.Type.UINT64
            case "Float":
                return ydb_value.Type.FLOAT
            case "Double":
                return ydb_value.Type.DOUBLE
            case "Date":
                return ydb_value.Type.DATE
            case "Datetime":
                return ydb_value.Type.DATETIME
            case "Timestamp":
                return ydb_value.Type.TIMESTAMP
            case _:
                raise Exception(f'invalid type: {src}')

    _epoch_start_date = datetime.date(1970, 1, 1)
    _epoch_start_datetime = datetime.datetime(1970, 1, 1)

    def cast(self, value: str) -> Any:
        match self.ydb_type.WhichOneof('type'):
            case 'type_id':
                return self.__cast_primitive_type(self.ydb_type.type_id, value)
            case 'optional_type':
                if value is None:  # kqprun return None if value is not presented
                    return None
                elif isinstance(value, YsonEntity):  # dqrun return YsonEntity if value is not presented
                    if value == None:  # noqa
                        return None

                    raise ValueError(f'unexpected YSONEntity: {value}')
                elif isinstance(value, list):  # dqrun return list with presented value
                    return self.__cast_primitive_type(self.ydb_type.optional_type.item.type_id, value[0])
                else:  # kqprun return presented value
                    return self.__cast_primitive_type(self.ydb_type.optional_type.item.type_id, value)

            case _:
                raise Exception(f'invalid type: {self.ydb_type}')

    def __cast_primitive_type(self, primitive_type_id: ydb_value.Type.PrimitiveTypeId, value: str) -> Any:
        match primitive_type_id:
            case ydb_value.Type.BOOL:
                return value
            case ydb_value.Type.UTF8:
                return value
            case ydb_value.Type.JSON:
                return value
            case ydb_value.Type.JSON_DOCUMENT:
                return value
            case ydb_value.Type.STRING:
                return value
            case ydb_value.Type.INT8:
                return int(value)
            case ydb_value.Type.INT16:
                return int(value)
            case ydb_value.Type.INT32:
                return int(value)
            case ydb_value.Type.INT64:
                return int(value)
            case ydb_value.Type.UINT8:
                return int(value)
            case ydb_value.Type.UINT16:
                return int(value)
            case ydb_value.Type.UINT32:
                return int(value)
            case ydb_value.Type.UINT64:
                return int(value)
            case ydb_value.Type.FLOAT:
                return float(value)
            case ydb_value.Type.DOUBLE:
                return float(value)
            case ydb_value.Type.DATE:
                return self._epoch_start_date + datetime.timedelta(days=int(value))
            case ydb_value.Type.DATETIME:
                return self._epoch_start_datetime + datetime.timedelta(seconds=int(value))
            case ydb_value.Type.TIMESTAMP:
                return self._epoch_start_datetime + datetime.timedelta(microseconds=int(value))
            case _:
                raise Exception(f"invalid type '{primitive_type_id}' for value '{value}'")

    def format_for_data_source(self, kind: EDataSourceKind.ValueType) -> str:
        return f'{self.name} {self.data_source_type.pick(kind)}'


class ColumnList(list):
    def __init__(self, *columns):
        for column in columns:
            if not (isinstance(column, Column)):
                raise Exception(f'invalid object: {column}')

        super().__init__(list(columns))

    @property
    def names(self) -> List[str]:
        return [col.name for col in self]

    @property
    def names_with_commas(self) -> str:
        return ", ".join((col.name for col in self))


class SelectWhat:
    """
    Represents entities queried from the data source.
    Emplaced into query right after SELECT keyword
    """

    @dataclass
    class Item:
        """
        A single item from the list of entities after SELECT keyword.
        """

        name: str
        alias: Optional[str] = None
        kind: str = 'col'  # col, expr

        def __str__(self) -> str:
            if self.alias:
                return self.alias

            return self.name if self.name != '*' else 'asterisk'

        @property
        def yql_name(self):
            if self.alias:
                return f'{self.name} AS {self.alias}'
            else:
                return self.name

        @property
        def is_expression(self):
            return self.kind == 'expr'

        @property
        def is_column(self):
            return self.kind == 'col'

        def name_qualified(self, table: str) -> str:
            if self.name == '*':
                raise ValueError('cannot use asterisk in this context')

            return f'{table}.{self.name}'

        def name_prefixed(self, table: str) -> str:
            if self.name == '*':
                raise ValueError('cannot use asterisk in this context')

            return f'{table}_{self.name}'

    # columns actually requested
    items: Sequence[Item]
    # columns stored in the table
    columns: Optional[ColumnList] = None

    def __init__(self, *items: Item):
        for item in items:
            if not isinstance(item, self.Item):
                raise ValueError('constructor argument must be an Item')

        self.items = items

    @classmethod
    def asterisk(cls, column_list: ColumnList):
        '''
        SELECT * FROM table
        '''
        out = cls(cls.Item(name='*'))
        out.columns = column_list
        return out

    @property
    def yql_select_names(self) -> str:
        return ", ".join(map(lambda i: i.yql_name, self.items))

    def names_with_prefix(self, table_name: str) -> str:
        return ", ".join(
            (f'{item.name_qualified(table_name)} AS {item.name_prefixed(table_name)}' for item in self.items)
        )

    @property
    def order_by_column_name(self) -> str:
        i = self.items[0]
        if i.name != '*' and i.is_column:
            return i.alias if i.alias else i.name

        if self.columns:
            return self.columns[0].name

        return ''

    def __str__(self) -> str:
        return "_".join((str(item) for item in self.items))


@dataclass
class SelectWhere:
    """
    Represents filter for query. Can handle dynamic arguments with keywords.
    Put this into expression if you want:

    * {cluster_name}
    * {table_name}
    """

    expression_: str  # filter expression or a template for it

    def render(self, cluster_name: str = None, table_name: str = None) -> str:
        """
        Renders expression template into final form.
        """
        return self.expression_.format(cluster_name=cluster_name, table_name=table_name)


@dataclass
class Schema:
    """
    The sequence of columns with names and types describing the table schema.
    """

    columns: ColumnList

    def cast_row(self, src: List) -> List:
        result = []
        for i, val in enumerate(src):
            result.append(self.columns[i].cast(val))

        return result

    @classmethod
    def from_yson(cls, src: YsonList):
        return cls(columns=ColumnList(*map(Column.from_yson, src)))

    @classmethod
    def from_json(cls, src: Dict):
        return cls(columns=ColumnList(*map(Column.from_json, src)))

    def yql_column_list(self, kind: EDataSourceKind.ValueType) -> str:
        return ", ".join(map(lambda col: col.format_for_data_source(kind), self.columns))

    def select_every_column(self) -> SelectWhat:
        '''
        Allows to query every column in a table:

        SELECT a, b, c, ... FROM table
        '''
        items = [SelectWhat.Item(name=col.name) for col in self.columns]
        return SelectWhat(*items)


def makeYdbTypeFromTypeID(type_id: Type.PrimitiveTypeId) -> Type:
    return Type(type_id=type_id)


def makeOptionalYdbTypeFromTypeID(type_id: Type.PrimitiveTypeId) -> Type:
    return Type(optional_type=OptionalType(item=Type(type_id=type_id)))


def makeOptionalYdbTypeFromYdbType(ydb_type: Type) -> Type:
    if ydb_type.HasField('optional_type'):
        return ydb_type
    return Type(optional_type=OptionalType(item=Type(type_id=ydb_type.type_id)))
