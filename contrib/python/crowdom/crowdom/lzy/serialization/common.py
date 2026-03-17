# We use Protobuf to serialize objects on lzy whiteboards.
# Standard pickle serialization does not fit our needs, because in general case refactorings in library code may lead
# to incorrect objects deserialization for old, already persisted whiteboards.
#
# For each class which will be stored in whiteboards, we define its serialization wrapper. A possible alternative is
# to provide additional metadata (@message class decorator, attributes data, etc.) to source classes, but we fully
# separate serialization logic from pure business-logic classes, because:
# 1) Currently we have issues with `pure-protobuf` dependency in Arcadia contrib.
# 2) Some attributes types, i.e. dicts, are not Protobuf serializable, and we don't want to modify source classes only
#    because of serialization needs.
#
# To serialize classes from inheritance schemes, we use Protobuf one_of.
#
# We also have another case, when client defines his own classes. Such sets of classes are limited (i.e. subclasses of
# Class) and we typically know data of these classes (.value for Class inheritor). But we don't know to which class
# we need to deserialize this data. So we introduce classes registry, and the client have to associate a permanent
# name with each of his self-defined class, so we can later instantiate this concrete class by its name.

import abc
from typing import Any, Callable, Dict, Generic, Type, TypeVar

from ... import base, objects


T = TypeVar('T', bound=Type[Any], covariant=True)
ClassT = TypeVar('ClassT', bound=Type[base.Class], covariant=True)

# DO NOT CHANGE THIS NAMES!
# They are persisted in lzy whiteboards and must remain the same for correct deserialization.
types_registry: Dict[Type[base.Object], str] = {
    base.SbSChoice: 'SbSChoice',
    base.BinaryEvaluation: 'BinaryEvaluation',
    objects.Audio: 'Audio',
    objects.Image: 'Image',
    objects.Text: 'Text',
    objects.Video: 'Video',
}
types_registry_reversed = {name: type for type, name in types_registry.items()}


def register_type(type: Type[base.Object], name: str):
    global types_registry, types_registry_reversed
    types_registry[type] = name
    types_registry_reversed[name] = type


func_registry = {}
func_registry_reversed = {}


def register_func(func: Callable, name: str):
    global func_registry, func_registry_reversed
    func_registry[func] = name
    func_registry_reversed[name] = func


# Most likely we won't need exact version of function as it was implemented at the time of the launch.
# Its outputs will be persisted in whiteboard anyway. So if we can't find it, it's not a problem.
def func_not_found(name: str) -> Callable:
    def f():
        raise RuntimeError(f'function "{name}" not found')

    return f


def load_func(name: str) -> Callable:
    return func_registry_reversed.get(name, func_not_found(name))


def deserialize_one_of_field(field):
    one_of = getattr(field, field.which_one_of)
    if isinstance(one_of, ProtobufSerializer):
        return one_of.deserialize()
    else:
        return one_of  # primitive type


class ProtobufSerializer(Generic[T]):
    @staticmethod
    @abc.abstractmethod
    def serialize(obj: T) -> 'ProtobufSerializer':
        ...

    @abc.abstractmethod
    def deserialize(self) -> T:
        ...
