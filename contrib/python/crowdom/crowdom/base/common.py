from dataclasses import dataclass, fields
from enum import Enum
from typing import Dict, Union

DEFAULT_LANG = 'EN'


# ISO 639-1 for language codes, as in Toloka
@dataclass(frozen=True)
class LocalizedString:
    lang_to_text: Dict[str, str]

    def __getitem__(self, lang: str) -> str:
        if lang not in self.lang_to_text:
            lang = DEFAULT_LANG
        assert lang in self.lang_to_text, f'no text for {lang} language'
        return self.lang_to_text[lang]

    def __add__(self, other: Union['LocalizedString', str]) -> 'LocalizedString':
        if isinstance(other, str):
            return LocalizedString({lang: text + other for lang, text in self.lang_to_text.items()})
        return LocalizedString(
            {lang: self[lang] + other[lang] for lang in self.lang_to_text.keys() | other.lang_to_text.keys()}
        )

    @staticmethod
    def convert_data(data: Union[Dict[str, str], 'LocalizedString']) -> 'LocalizedString':
        if isinstance(data, LocalizedString):
            return data
        return LocalizedString(data)

    @staticmethod
    def convert_dataclass(obj: dataclass) -> None:
        for f in fields(obj):
            if f.type == LocalizedString:
                setattr(obj, f.name, LocalizedString.convert_data(getattr(obj, f.name)))


EMPTY_STRING = LocalizedString({DEFAULT_LANG: ''})


class TextFormat(Enum):
    PLAIN = 'plain'
    MARKDOWN = 'markdown'


@dataclass
class Title:
    text: LocalizedString
    format: TextFormat = TextFormat.PLAIN

    def __post_init__(self):
        LocalizedString.convert_dataclass(self)
