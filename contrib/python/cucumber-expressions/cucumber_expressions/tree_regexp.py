import re

from cucumber_expressions.ast import EscapeCharacters
from cucumber_expressions.group_builder import GroupBuilder


class TreeRegexp:
    def __init__(self, regexp: str):
        self.regexp = regexp if isinstance(regexp, re.Pattern) else re.compile(regexp)
        self._group_builder = None
        if not self._group_builder:
            self._group_builder = self.create_group_builder(self.regexp)

    def match(self, string: str):
        matches = self.regexp.match(string)
        if not matches:
            return None
        group_indices = range(len(matches.groups()) + 1)
        return self.group_builder.build(matches, iter(group_indices))

    def create_group_builder(self, regexp):
        source = regexp.pattern
        stack: list[GroupBuilder] = [GroupBuilder()]
        group_start_stack = []
        escaping: bool = False
        char_class: bool = False
        for index, char in enumerate(source):
            if char == "[" and not escaping:
                char_class = True
            elif char == "]" and not escaping:
                char_class = False
            elif char == "(" and not escaping and not char_class:
                group_start_stack.append(index)
                group_builder = GroupBuilder()
                if self.is_non_capturing(source, index):
                    group_builder.capturing = False
                stack.append(group_builder)
            elif char == ")" and not escaping and not char_class:
                group_builder = stack.pop()
                if not group_builder:
                    raise Exception("Empty stack!")
                group_start = group_start_stack.pop()
                group_start = group_start or 0
                if group_builder.capturing:
                    group_builder.source = source[(group_start + 1) : index]
                    stack[-1].add(group_builder)
                else:
                    group_builder.move_children_to(stack[-1])
            escaping = not escaping and char == EscapeCharacters.ESCAPE_CHARACTER.value
        return stack.pop()

    @staticmethod
    def is_non_capturing(source, index):
        # Regex is valid. Bounds check not required.
        if source[index + 1] != "?":
            # (X)
            return False
        if source[index + 2] != "<":
            # (?:X)
            # (?idmsuxU-idmsuxU)
            # (?idmsux-idmsux:X)
            # (?=X)
            # (?!X)
            # (?>X)
            return True
        # (?<=X) or (?<!X) else (?<name>X)
        return source[index + 3] in ["=", "!"]

    @property
    def group_builder(self):
        return self._group_builder
