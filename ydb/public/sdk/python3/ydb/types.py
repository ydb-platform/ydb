# -*- coding: utf-8 -*-
import abc
import enum
import json
from . import _utilities, _apis
from datetime import date, datetime, timedelta
import uuid
import struct
from google.protobuf import struct_pb2


_SECONDS_IN_DAY = 60 * 60 * 24
_EPOCH = datetime(1970, 1, 1)


def _from_date(x, table_client_settings):
    if table_client_settings is not None and table_client_settings._native_date_in_result_sets:
        return _EPOCH.date() + timedelta(days=x.uint32_value)
    return x.uint32_value


def _to_date(pb, value):
    if isinstance(value, date):
        pb.uint32_value = (value - _EPOCH.date()).days
    else:
        pb.uint32_value = value


def _from_datetime_number(x, table_client_settings):
    if table_client_settings is not None and table_client_settings._native_datetime_in_result_sets:
        return datetime.utcfromtimestamp(x)
    return x


def _from_json(x, table_client_settings):
    if table_client_settings is not None and table_client_settings._native_json_in_result_sets:
        return json.loads(x)
    return x


def _to_uuid(value_pb, table_client_settings):
    return uuid.UUID(bytes_le=struct.pack("QQ", value_pb.low_128, value_pb.high_128))


def _from_uuid(pb, value):
    pb.low_128 = struct.unpack("Q", value.bytes_le[0:8])[0]
    pb.high_128 = struct.unpack("Q", value.bytes_le[8:16])[0]


def _from_interval(value_pb, table_client_settings):
    if table_client_settings is not None and table_client_settings._native_interval_in_result_sets:
        return timedelta(microseconds=value_pb.int64_value)
    return value_pb.int64_value


def _timedelta_to_microseconds(value):
    return (value.days * _SECONDS_IN_DAY + value.seconds) * 1000000 + value.microseconds


def _to_interval(pb, value):
    if isinstance(value, timedelta):
        pb.int64_value = _timedelta_to_microseconds(value)
    else:
        pb.int64_value = value


def _from_timestamp(value_pb, table_client_settings):
    if table_client_settings is not None and table_client_settings._native_timestamp_in_result_sets:
        return _EPOCH + timedelta(microseconds=value_pb.uint64_value)
    return value_pb.uint64_value


def _to_timestamp(pb, value):
    if isinstance(value, datetime):
        pb.uint64_value = _timedelta_to_microseconds(value - _EPOCH)
    else:
        pb.uint64_value = value


@enum.unique
class PrimitiveType(enum.Enum):
    """
    Enumerates all available primitive types that can be used
    in computations.
    """

    Int32 = _apis.primitive_types.INT32, "int32_value"
    Uint32 = _apis.primitive_types.UINT32, "uint32_value"
    Int64 = _apis.primitive_types.INT64, "int64_value"
    Uint64 = _apis.primitive_types.UINT64, "uint64_value"
    Int8 = _apis.primitive_types.INT8, "int32_value"
    Uint8 = _apis.primitive_types.UINT8, "uint32_value"
    Int16 = _apis.primitive_types.INT16, "int32_value"
    Uint16 = _apis.primitive_types.UINT16, "uint32_value"
    Bool = _apis.primitive_types.BOOL, "bool_value"
    Double = _apis.primitive_types.DOUBLE, "double_value"
    Float = _apis.primitive_types.FLOAT, "float_value"

    String = _apis.primitive_types.STRING, "bytes_value"
    Utf8 = _apis.primitive_types.UTF8, "text_value"

    Yson = _apis.primitive_types.YSON, "bytes_value"
    Json = _apis.primitive_types.JSON, "text_value", _from_json
    JsonDocument = _apis.primitive_types.JSON_DOCUMENT, "text_value", _from_json
    UUID = (_apis.primitive_types.UUID, None, _to_uuid, _from_uuid)
    Date = (
        _apis.primitive_types.DATE,
        None,
        _from_date,
        _to_date,
    )
    Datetime = (
        _apis.primitive_types.DATETIME,
        "uint32_value",
        _from_datetime_number,
    )
    Timestamp = (
        _apis.primitive_types.TIMESTAMP,
        None,
        _from_timestamp,
        _to_timestamp,
    )
    Interval = (
        _apis.primitive_types.INTERVAL,
        None,
        _from_interval,
        _to_interval,
    )

    DyNumber = _apis.primitive_types.DYNUMBER, "text_value"

    def __init__(self, idn, proto_field, to_obj=None, from_obj=None):
        self._idn_ = idn
        self._to_obj = to_obj
        self._from_obj = from_obj
        self._proto_field = proto_field

    def get_value(self, value_pb, table_client_settings):
        """
        Extracts value from protocol buffer
        :param value_pb: A protocol buffer
        :return: A valid value of primitive type
        """
        if self._to_obj is not None and self._proto_field:
            return self._to_obj(getattr(value_pb, self._proto_field), table_client_settings)

        if self._to_obj is not None:
            return self._to_obj(value_pb, table_client_settings)

        return getattr(value_pb, self._proto_field)

    def set_value(self, pb, value):
        """
        Sets value in a protocol buffer
        :param pb: A protocol buffer
        :param value: A valid value to set
        :return: None
        """
        if self._from_obj:
            self._from_obj(pb, value)
        else:
            setattr(pb, self._proto_field, value)

    def __str__(self):
        return self._name_

    @property
    def proto(self):
        """
        Returns protocol buffer representation of a primitive type
        :return: A protocol buffer representation
        """
        return _apis.ydb_value.Type(type_id=self._idn_)


class DataQuery(object):
    __slots__ = ("yql_text", "parameters_types", "name")

    def __init__(self, query_id, parameters_types, name=None):
        self.yql_text = query_id
        self.parameters_types = parameters_types
        self.name = _utilities.get_query_hash(self.yql_text) if name is None else name


#######################
# A deprecated alias  #
#######################
DataType = PrimitiveType


class AbstractTypeBuilder(object):
    __metaclass__ = abc.ABCMeta

    @property
    @abc.abstractmethod
    def proto(self):
        """
        Returns protocol buffer representation of a type
        :return: A protocol buffer representation
        """
        pass


class DecimalType(AbstractTypeBuilder):
    __slots__ = ("_proto", "_precision", "_scale")

    def __init__(self, precision=22, scale=9):
        """
        Represents a decimal type
        :param precision: A precision value
        :param scale: A scale value
        """
        self._precision = precision
        self._scale = scale
        self._proto = _apis.ydb_value.Type()
        self._proto.decimal_type.MergeFrom(_apis.ydb_value.DecimalType(precision=self._precision, scale=self._scale))

    @property
    def precision(self):
        return self._precision

    @property
    def scale(self):
        return self._scale

    @property
    def proto(self):
        """
        Returns protocol buffer representation of a type
        :return: A protocol buffer representation
        """
        return self._proto

    def __eq__(self, other):
        return self._precision == other.precision and self._scale == other.scale

    def __str__(self):
        """
        Returns string representation of a type
        :return: A string representation
        """
        return "Decimal(%d,%d)" % (self._precision, self._scale)


class NullType(AbstractTypeBuilder):
    __slots__ = ("_repr", "_proto")

    def __init__(self):
        self._proto = _apis.ydb_value.Type(null_type=struct_pb2.NULL_VALUE)

    @property
    def proto(self):
        return self._proto

    def __str__(self):
        return "NullType"


class OptionalType(AbstractTypeBuilder):
    __slots__ = ("_repr", "_proto", "_item")

    def __init__(self, optional_type):
        """
        Represents optional type that wraps inner type
        :param optional_type: An instance of an inner type
        """
        self._repr = "%s?" % str(optional_type)
        self._proto = _apis.ydb_value.Type()
        self._item = optional_type
        self._proto.optional_type.MergeFrom(_apis.ydb_value.OptionalType(item=optional_type.proto))

    @property
    def item(self):
        return self._item

    @property
    def proto(self):
        """
        Returns protocol buffer representation of a type
        :return: A protocol buffer representation
        """
        return self._proto

    def __eq__(self, other):
        return self._item == other.item

    def __str__(self):
        return self._repr


class ListType(AbstractTypeBuilder):
    __slots__ = ("_repr", "_proto")

    def __init__(self, list_type):
        """
        :param list_type: List item type builder
        """
        self._repr = "List<%s>" % str(list_type)
        self._proto = _apis.ydb_value.Type(list_type=_apis.ydb_value.ListType(item=list_type.proto))

    @property
    def proto(self):
        """
        Returns protocol buffer representation of type
        :return: A protocol buffer representation
        """
        return self._proto

    def __str__(self):
        return self._repr


class DictType(AbstractTypeBuilder):
    __slots__ = ("__repr", "__proto")

    def __init__(self, key_type, payload_type):
        """
        :param key_type: Key type builder
        :param payload_type: Payload type builder
        """
        self._repr = "Dict<%s,%s>" % (str(key_type), str(payload_type))
        self._proto = _apis.ydb_value.Type(
            dict_type=_apis.ydb_value.DictType(
                key=key_type.proto,
                payload=payload_type.proto,
            )
        )

    @property
    def proto(self):
        return self._proto

    def __str__(self):
        return self._repr


class TupleType(AbstractTypeBuilder):
    __slots__ = ("__elements_repr", "__proto")

    def __init__(self):
        self.__elements_repr = []
        self.__proto = _apis.ydb_value.Type(tuple_type=_apis.ydb_value.TupleType())

    def add_element(self, element_type):
        """
        :param element_type: Adds additional element of tuple
        :return: self
        """
        self.__elements_repr.append(str(element_type))
        element = self.__proto.tuple_type.elements.add()
        element.MergeFrom(element_type.proto)
        return self

    @property
    def proto(self):
        return self.__proto

    def __str__(self):
        return "Tuple<%s>" % ",".join(self.__elements_repr)


class StructType(AbstractTypeBuilder):
    __slots__ = ("__members_repr", "__proto")

    def __init__(self):
        self.__members_repr = []
        self.__proto = _apis.ydb_value.Type(struct_type=_apis.ydb_value.StructType())

    def add_member(self, name, member_type):
        """
        :param name:
        :param member_type:
        :return:
        """
        self.__members_repr.append("%s:%s" % (name, str(member_type)))
        member = self.__proto.struct_type.members.add()
        member.name = name
        member.type.MergeFrom(member_type.proto)
        return self

    @property
    def proto(self):
        return self.__proto

    def __str__(self):
        return "Struct<%s>" % ",".join(self.__members_repr)


class BulkUpsertColumns(AbstractTypeBuilder):
    __slots__ = ("__columns_repr", "__proto")

    def __init__(self):
        self.__columns_repr = []
        self.__proto = _apis.ydb_value.Type(struct_type=_apis.ydb_value.StructType())

    def add_column(self, name, column_type):
        """
        :param name: A column name
        :param column_type: A column type
        """
        self.__columns_repr.append("%s:%s" % (name, column_type))
        column = self.__proto.struct_type.members.add()
        column.name = name
        column.type.MergeFrom(column_type.proto)
        return self

    @property
    def proto(self):
        return self.__proto

    def __str__(self):
        return "BulkUpsertColumns<%s>" % ",".join(self.__columns_repr)
