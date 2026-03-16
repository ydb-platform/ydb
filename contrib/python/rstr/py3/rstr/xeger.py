import random
import string
from itertools import chain
import typing
from typing import Any, Callable, Dict, Mapping, Pattern, Sequence, Union

from rstr.rstr_base import RstrBase

if typing.TYPE_CHECKING:
    from random import Random

try:
    import re._parser as sre_parse  # type: ignore[import-not-found]
except ImportError:  # Python < 3.11
    import sre_parse


# The * and + characters in a regular expression
# match up to any number of repeats in theory,
# (and actually 65535 repeats in python) but you
# probably don't want that many repeats in your
# generated strings. This sets an upper-bound on
# repeats generated from + and * characters.
STAR_PLUS_LIMIT = 100


class Xeger(RstrBase):
    '''Inspired by the Java library Xeger: http://code.google.com/p/xeger/
    This class adds functionality to Rstr allowing users to generate a
    semi-random string from a regular expression.'''

    def __init__(
        self, _random: 'Random' = typing.cast('Random', random), **custom_alphabets: str,
    ) -> None:
        super().__init__(_random, **custom_alphabets)
        self._cache: Dict[str, str] = {}
        self._categories: Mapping[str, Callable[[], str]] = {
            'category_digit': lambda: self._alphabets['digits'],
            'category_not_digit': lambda: self._alphabets['nondigits'],
            'category_space': lambda: self._alphabets['whitespace'],
            'category_not_space': lambda: self._alphabets['nonwhitespace'],
            'category_word': lambda: self._alphabets['word'],
            'category_not_word': lambda: self._alphabets['nonword'],
        }

        self._cases: Mapping[str, Callable[..., Any]] = {
            'literal': lambda x: chr(x),
            'not_literal': lambda x: self._random.choice(string.printable.replace(chr(x), '')),
            'at': lambda x: '',
            'in': lambda x: self._handle_in(x),
            'any': lambda x: self.printable(1, exclude='\n'),
            'range': lambda x: [chr(i) for i in range(x[0], x[1] + 1)],
            'category': lambda x: self._categories[x](),
            'branch': lambda x: ''.join(self._handle_state(i) for i in self._random.choice(x[1])),
            'subpattern': lambda x: self._handle_group(x),
            'assert': lambda x: ''.join(self._handle_state(i) for i in x[1]),
            'assert_not': lambda x: '',
            'groupref': lambda x: self._cache[x],
            'min_repeat': lambda x: self._handle_repeat(*x),
            'max_repeat': lambda x: self._handle_repeat(*x),
            'negate': lambda x: [False],
        }

    def xeger(self, string_or_regex: Union[str, Pattern[str]]) -> str:
        try:
            pattern = typing.cast(Pattern[str], string_or_regex).pattern
        except AttributeError:
            pattern = typing.cast(str, string_or_regex)

        parsed = sre_parse.parse(pattern)
        result = self._build_string(parsed)
        self._cache.clear()
        return result

    def _build_string(self, parsed: Any) -> str:
        newstr = []
        for state in parsed:
            newstr.append(self._handle_state(state))
        return ''.join(newstr)

    def _handle_state(self, state: Any) -> Any:
        opcode, value = state
        opcode = opcode.name.lower()
        if opcode == 'category':
            value = value.name.lower()
        return self._cases[opcode](value)

    def _handle_group(self, value: Sequence[Any]) -> str:
        result = ''.join(self._handle_state(i) for i in value[-1])
        if value[0]:
            self._cache[value[0]] = result
        return result

    def _handle_in(self, value: Any) -> Any:
        candidates = list(chain(*(self._handle_state(i) for i in value)))
        if candidates[0] is False:
            candidates = list(set(string.printable).difference(candidates[1:]))
        return self._random.choice(candidates)

    def _handle_repeat(self, start_range: int, end_range: int, value: str) -> str:
        result = []
        end_range = min((end_range, STAR_PLUS_LIMIT))
        times = self._random.randint(start_range, end_range)
        for i in range(times):
            result.append(''.join(self._handle_state(i) for i in value))
        return ''.join(result)
