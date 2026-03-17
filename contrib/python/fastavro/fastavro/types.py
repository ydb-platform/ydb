import decimal
from typing import Union, List, Dict, Any

AvroMessage = Union[
    None,  # 'null' Avro type
    str,  # 'string' and 'enum'
    float,  # 'float' and 'double'
    int,  # 'int' and 'long'
    decimal.Decimal,  # 'fixed'
    bool,  # 'boolean'
    bytes,  # 'bytes'
    List[Any],  # 'array'
    Dict[Any, Any],  # 'map' and 'record'
]
DictSchema = Dict[Any, Any]
Schema = Union[str, List[Any], DictSchema]
NamedSchemas = Dict[str, Dict[Any, Any]]
