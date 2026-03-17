import abc
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import List, Dict, Any

from ..precision import FLOAT_PRECISION_PLACES
from .common import DEFAULT_LANG, LocalizedString


class Object:
    @staticmethod
    def is_media() -> bool:
        return False


class Label:
    @staticmethod
    @abc.abstractmethod
    def possible_values() -> list:
        ...

    @staticmethod
    @abc.abstractmethod
    def possible_instances() -> List['Label']:
        ...

    @staticmethod
    @abc.abstractmethod
    def get_display_labels() -> List[LocalizedString]:
        ...


class Class(Label, Object, Enum):
    @classmethod
    def possible_values(cls) -> List[str]:
        return [i.value for i in cls.possible_instances()]

    @classmethod
    def possible_instances(cls) -> List['Class']:
        return list(cls)

    @classmethod
    def get_display_labels(cls) -> List[LocalizedString]:
        return [i.get_label() for i in cls.possible_instances()]

    def get_label(self) -> LocalizedString:
        return LocalizedString(type(self).labels()[self])

    @classmethod
    def labels(cls) -> Dict['Class', Dict[str, str]]:
        return {value: {DEFAULT_LANG: value.value} for value in cls.possible_instances()}

    def __lt__(self, other):
        if isinstance(other, Class):
            return self.value < other.value
        # during crowd-kit Dawid-Skene aggregation values are sometimes compared to column names
        assert isinstance(other, str)
        return self.value < other

    def __new__(cls, *args):
        # todo: subclass Class from StrEnum, when it becomes available
        value = args[0]
        assert isinstance(value, str), 'only str-valued Classes are accepted'
        obj = object.__new__(cls)
        obj._value_ = value
        return obj


class SbSChoice(Class):
    A = 'a'
    B = 'b'

    @classmethod
    def labels(cls) -> Dict['SbSChoice', Dict[str, str]]:
        return {
            cls.A: {
                'EN': 'Left option',
                'RU': 'Левый вариант',
                'TR': 'Sol seçenek',
                'KK': 'Сол жақтағы опция',
                'UZ': 'Chap variant',
                'ES': 'Opción de mano izquierda',
            },
            cls.B: {
                'EN': 'Right option',
                'RU': 'Правый вариант',
                'TR': 'Doğru seçenek',
                'KK': 'Оң жақтағы опция',
                'UZ': 'Oʻng variant',
                'ES': 'Opción de mano derecha',
            },
        }

    def swap(self) -> 'SbSChoice':
        if self == self.A:
            return self.B
        return self.A


class Evaluation(Object):
    ...


@dataclass(eq=True, order=True, frozen=True)
class BinaryEvaluation(Evaluation, Label):
    ok: bool

    @staticmethod
    def possible_values() -> List[bool]:
        return [True, False]

    @staticmethod
    def possible_instances() -> List['BinaryEvaluation']:
        return [BinaryEvaluation(ok=value) for value in BinaryEvaluation.possible_values()]

    @staticmethod
    def get_display_labels() -> List[LocalizedString]:
        return [
            LocalizedString({'EN': 'Yes', 'RU': 'Да', 'TR': 'Evet', 'KK': 'Иә'}),
            LocalizedString({'EN': 'No', 'RU': 'Нет', 'TR': 'Hayır', 'KK': 'Жоқ'}),
        ]

    @staticmethod
    def get_ok_confidence(probas: Dict['BinaryEvaluation', float]) -> float:
        assert len(probas), 'empty probas for binary evaluation'
        if BinaryEvaluation(ok=True) in probas:
            return probas[BinaryEvaluation(ok=True)]
        return 1 - probas[BinaryEvaluation(ok=False)]


class ScoreEvaluation(Evaluation, Class):
    ...


@dataclass(frozen=True)
class Metadata(Object):
    metadata: str


def _change_type(
    data: Any,
    from_type: type,
    to_type: type,
    change_precision: bool = False,
) -> Any:
    if isinstance(data, dict):
        return {key: _change_type(value, from_type, to_type, change_precision) for key, value in data.items()}
    elif isinstance(data, list):
        return [_change_type(item, from_type, to_type, change_precision) for item in data]
    elif isinstance(data, from_type):
        data = to_type(data)
    if isinstance(data, to_type) and change_precision:
        data = round(data, FLOAT_PRECISION_PLACES)
    return data


@dataclass(init=False, frozen=True)
class ImageAnnotation(Object):
    data: list

    def __init__(self, data: list):
        object.__setattr__(self, 'data', _change_type(data, float, Decimal))

    def __repr__(self) -> str:
        return f"{type(self).__name__}(data={_change_type(self.data, float, Decimal, change_precision=True)})"

    def __hash__(self) -> int:
        return hash(repr(self))
