
import re

from .record import Record
from .check import assert_type
from .span import Span
from .token import Token


class TokenRule(Record):
    __attributes__ = ['type', 'pattern']

    def __init__(self, type, pattern):
        self.type = type
        self.pattern = pattern


RUSSIAN = 'RU'
LATIN = 'LATIN'
INT = 'INT'
PUNCT = 'PUNCT'
EOL = 'EOL'
OTHER = 'OTHER'

EMAIL_RULE = TokenRule(
    'EMAIL',
    r'[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+'
)
# https://toster.ru/answer?answer_id=852265#answers_list_answer
PHONE_RULE = TokenRule(
    'PHONE',
    r'(\+)?([-\s_()]?\d[-\s_()]?){10,14}'
)

GENERAL_QUOTES = '"\'”'
LEFT_QUOTES = '«„ʼ'
RIGHT_QUOTES = '»“ʻ'
QUOTES = LEFT_QUOTES + GENERAL_QUOTES + RIGHT_QUOTES

RULES = [
    TokenRule(RUSSIAN, r'[а-яё]+'),
    TokenRule(LATIN, r'[a-z]+'),
    TokenRule(INT, r'\d+'),
    TokenRule(
        PUNCT,
        r'[-\\/!#$%&()\[\]\*\+,\.:;<=>?@^_`{|}~№…"\'«»„“ʼʻ”]'
    ),
    TokenRule(EOL, r'[\n\r]+'),
    TokenRule(OTHER, r'\S'),
]


class Tokenizer(object):
    def __init__(self, rules=RULES):
        self.reset(rules)

    def reset(self, rules):
        for rule in rules:
            assert_type(rule, TokenRule)
        self.rules = rules
        self.regexp, self.mapping, self.types = self.compile(rules)

    def add_rules(self, *rules):
        self.reset(list(rules) + self.rules)
        return self

    def remove_types(self, *types):
        for type in types:
            self.check_type(type)
        self.reset([
            _ for _ in self.rules
            if _.type not in types
        ])
        return self

    def check_type(self, type):
        if type not in self.types:
            raise ValueError(type)

    def compile(self, rules):
        types = set()
        mapping = {}
        patterns = []
        for rule in rules:
            type, pattern = rule
            name = 'rule_{id}'.format(id=id(rule))
            pattern = r'(?P<{name}>{pattern})'.format(
                name=name,
                pattern=pattern
            )
            mapping[name] = type
            types.add(type)
            patterns.append(pattern)
        pattern = '|'.join(patterns)
        regexp = re.compile(pattern, re.UNICODE | re.IGNORECASE)
        return regexp, mapping, types

    def __call__(self, text):
        for match in re.finditer(self.regexp, text):
            name = match.lastgroup
            value = match.group(0)
            start, stop = match.span()
            type = self.mapping[name]
            token = Token(value, Span(start, stop), type)
            yield token

    def split(self, text):
        return [_.value for _ in self(text)]


class MorphTokenizer(Tokenizer):
    def __init__(self, rules=RULES, morph=None):
        super(MorphTokenizer, self).__init__(rules)
        if not morph:
            from .morph import CachedMorphAnalyzer
            morph = CachedMorphAnalyzer()
        self.morph = morph

    def __call__(self, text):
        tokens = Tokenizer.__call__(self, text)
        for token in tokens:
            if token.type == RUSSIAN:
                forms = self.morph(token.value)
                yield token.morphed(forms)
            else:
                yield token
