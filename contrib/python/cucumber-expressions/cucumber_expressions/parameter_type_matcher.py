from __future__ import annotations

import re
from re import Pattern

from cucumber_expressions.parameter_type import ParameterType


class ParameterTypeMatcher:
    def __init__(
        self,
        parameter_type: ParameterType,
        regexp: str | Pattern,
        text: str,
        match_position: int = 0,
    ):
        self.parameter_type = parameter_type
        self.regexp: Pattern = (
            regexp if isinstance(regexp, Pattern) else re.compile(regexp)
        )
        self.text = text
        self.match_position = match_position
        _matches = self.regexp.search(self.text[self.match_position :])
        self.match = _matches.regs[0] if _matches else None

    def advance_to(self, new_match_position: int) -> ParameterTypeMatcher:
        for advanced_position in range(new_match_position, len(self.text)):
            matcher = ParameterTypeMatcher(
                self.parameter_type,
                self.regexp,
                self.text,
                advanced_position,
            )
            if matcher.find and matcher.full_word:
                return matcher
        return ParameterTypeMatcher(
            self.parameter_type,
            self.regexp,
            self.text,
            len(self.text),
        )

    @property
    def find(self) -> bool:
        return bool(self.match and self.group)

    @property
    def full_word(self) -> bool:
        return bool(self.match_start_word() and self.match_end_word())

    @property
    def start(self) -> int:
        return self.match_position + self.match[0]

    @property
    def end(self) -> int:
        return self.start + len(self.group)

    @property
    def group(self):
        return self.text[self.match_position :][self.match[0] : self.match[1]]

    @staticmethod
    def compare(
        this_object: ParameterTypeMatcher,
        that_object: ParameterTypeMatcher,
    ) -> int:
        pos_comparison = this_object.start - that_object.start
        if pos_comparison != 0:
            return pos_comparison
        length_comparison = len(that_object.group) - len(this_object.group)
        if length_comparison != 0:
            return length_comparison
        return 0

    def match_start_word(self) -> bool:
        return bool(
            self.start == 0 or not self.text[self.start - 1 : self.start].isalnum(),
        )

    def match_end_word(self) -> bool:
        return bool(
            self.end == len(self.text)
            or not self.text[self.end : self.end + 1].isalnum(),
        )
