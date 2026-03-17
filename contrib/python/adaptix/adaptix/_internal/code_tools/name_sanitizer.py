import re
import string
from abc import ABC, abstractmethod


class NameSanitizer(ABC):
    @abstractmethod
    def sanitize(self, name: str) -> str:
        ...


class BuiltinNameSanitizer(NameSanitizer):
    _BAD_CHARS = re.compile(r"\W")
    _TRANSLATE_MAP = str.maketrans({".": "_", "[": "_"})

    def sanitize(self, name: str) -> str:
        if name == "":
            return ""

        first_letter = name[0] if name[0] in string.ascii_letters else "_"
        return first_letter + self._BAD_CHARS.sub("", name[1:].translate(self._TRANSLATE_MAP))
