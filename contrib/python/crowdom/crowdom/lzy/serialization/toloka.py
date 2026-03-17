import abc
from dataclasses import dataclass
from decimal import Decimal
import json
from typing import Generic, Type, TypeVar

from pure_protobuf.dataclasses_ import field, message
import toloka.client as toloka

from ...utils import DecimalEncoder
from .common import ProtobufSerializer

TolokaObj = toloka.primitives.base.BaseTolokaObject
TolokaObjT = TypeVar('TolokaObjT', bound=Type[TolokaObj], covariant=True)


@message
@dataclass
class TolokaObject(ProtobufSerializer[TolokaObj], Generic[TolokaObjT]):
    json: str = field(1)

    @classmethod
    def serialize(cls, obj: TolokaObj):
        return cls(json=json.dumps(obj.unstructure(), cls=DecimalEncoder))

    def deserialize(self) -> TolokaObj:
        return self.toloka_cls().structure(json.loads(self.json, parse_float=Decimal))

    @staticmethod
    @abc.abstractmethod
    def toloka_cls() -> Type[TolokaObj]:
        ...


@message
class TolokaPool(TolokaObject[toloka.Pool]):
    @staticmethod
    def toloka_cls() -> Type[toloka.Pool]:
        return toloka.Pool


@message
class TolokaFilterCondition(TolokaObject[toloka.filter.FilterCondition]):
    @staticmethod
    def toloka_cls() -> Type[toloka.filter.FilterCondition]:
        return toloka.filter.FilterCondition


@message
class TolokaSkill(TolokaObject[toloka.Skill]):
    @staticmethod
    def toloka_cls() -> Type[toloka.Skill]:
        return toloka.Skill


@message
class TolokaProject(TolokaObject[toloka.Project]):
    @staticmethod
    def toloka_cls() -> Type[toloka.Project]:
        return toloka.Project


@message
class TolokaAssignment(TolokaObject[toloka.Assignment]):
    @staticmethod
    def toloka_cls() -> Type[toloka.Assignment]:
        return toloka.Assignment
