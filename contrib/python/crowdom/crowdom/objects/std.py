from dataclasses import dataclass
from typing import Type, TypeVar, Optional

from crowdom.base import Object, ObjectMeta, TextFormat, LocalizedString


@dataclass(frozen=True)
class Text(Object):
    text: str


TextT = TypeVar('TextT', bound=Type[Text], covariant=True)


@dataclass
class TextValidation:
    regex: LocalizedString
    hint: LocalizedString

    def __post_init__(self):
        LocalizedString.convert_dataclass(self)


@dataclass
class TextMeta(ObjectMeta[TextT]):
    format: TextFormat = TextFormat.PLAIN
    validation: Optional[TextValidation] = None


@dataclass(frozen=True)
class Audio(Object):
    url: str

    @staticmethod
    def is_media() -> bool:
        return True


@dataclass(frozen=True)
class Image(Object):
    url: str

    @staticmethod
    def is_media() -> bool:
        return True


@dataclass(frozen=True)
class Video(Object):
    url: str

    @staticmethod
    def is_media() -> bool:
        return True
