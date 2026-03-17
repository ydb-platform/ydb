__all__ = [
    'AdditionalLanguage',
    'LocalizationConfig',
]
from enum import Enum, unique
from typing import List

from ..primitives.base import BaseTolokaObject
from ...util._codegen import attribute


class AdditionalLanguage(BaseTolokaObject):
    """A translation of a project interface.

    Args:
        language: The language into which the translation is made. Two-letter [ISO 639-1](https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes) language code in upper case.
        public_name: A translated project name.
        public_description: A translated project description.
        public_instructions: Translated instructions for Tolokers.
    """

    class FieldTranslation(BaseTolokaObject):
        """A translation of a text parameter.

        Args:
            value: A translated text.
            source: A translation origin.
        """

        @unique
        class Source(Enum):
            """A translation origin.

            The only value 'REQUESTER' is supported so far.
            """
            REQUESTER = 'REQUESTER'

        value: str
        source: Source = attribute(factory=lambda: AdditionalLanguage.FieldTranslation.Source.REQUESTER,
                                   autocast=True)

    language: str
    public_name: FieldTranslation
    public_description: FieldTranslation
    public_instructions: FieldTranslation


class LocalizationConfig(BaseTolokaObject):
    """All translations of a project interface.

    Args:
        default_language: The main language used for text parameters when the project was created. It is a required parameter.
        additional_languages: A list of translations to other languages.
    """
    default_language: str
    additional_languages: List[AdditionalLanguage] = attribute(factory=list)
