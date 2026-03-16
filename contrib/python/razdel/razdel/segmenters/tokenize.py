
import re

from razdel.record import (
    Record,
    cached_property
)
from razdel.rule import (
    JOIN, SPLIT,
    Rule,
    FunctionRule
)
from razdel.split import (
    Split,
    Splitter,
)

from .base import (
    safe_next,
    Segmenter,
    DebugSegmenter
)
from .punct import (
    DASHES,
    ENDINGS,
    QUOTES,
    BRACKETS,

    SMILES
)


RU = 'RU'
LAT = 'LAT'
INT = 'INT'
PUNCT = 'PUNCT'
OTHER = 'OTHER'

PUNCTS = '\\/!#$%&*+,.:;<=>?@^_`|~№…' + DASHES + QUOTES + BRACKETS

ATOM = re.compile(
    r'''
    (?P<RU>[а-яё]+)
    |(?P<LAT>[a-z]+)
    |(?P<INT>\d+)
    |(?P<PUNCT>[%s])
    |(?P<OTHER>\S)
    ''' % re.escape(PUNCTS),
    re.I | re.U | re.X
)

SMILE = re.compile(r'^' + SMILES + '$', re.U)


##########
#
#  TRIVIAL
#
######


def split_space(split):
    if split.delimiter:
        return SPLIT


##########
#
#   2112
#
##########


class Rule2112(Rule):
    def __call__(self, split):
        if self.delimiter(split.left):
            # cho-|to
            left, right = split.left_2, split.right_1
        elif self.delimiter(split.right):
            # cho|-to
            left, right = split.left_1, split.right_2
        else:
            return

        if not left or not right:
            return

        return self.rule(left, right)


class DashRule(Rule2112):
    name = 'dash'

    def delimiter(self, delimiter):
        return delimiter in DASHES

    def rule(self, left, right):
        if left.type == PUNCT or right.type == PUNCT:
            return
        return JOIN


class UnderscoreRule(Rule2112):
    name = 'underscore'

    def delimiter(self, delimiter):
        return delimiter == '_'

    def rule(self, left, right):
        if left.type == PUNCT or right.type == PUNCT:
            return
        return JOIN


class FloatRule(Rule2112):
    name = 'float'

    def delimiter(self, delimiter):
        return delimiter in '.,'

    def rule(self, left, right):
        if left.type == INT and right.type == INT:
            return JOIN


class FractionRule(Rule2112):
    name = 'fraction'

    def delimiter(self, delimiter):
        return delimiter in '/\\'

    def rule(self, left, right):
        if left.type == INT and right.type == INT:
            return JOIN


########
#
#   PUNCT
#
##########


def punct(split):
    if split.left_1.type != PUNCT or split.right_1.type != PUNCT:
        return

    left = split.left
    right = split.right

    if SMILE.match(split.buffer + right):
        return JOIN

    if left in ENDINGS and right in ENDINGS:
        # ... ?!
        return JOIN

    if left + right in ('--', '**'):
        # ***
        return JOIN


def other(split):
    left = split.left_1.type
    right = split.right_1.type

    if left == OTHER and right in (OTHER, RU, LAT):
        # ΔP
        return JOIN

    if left in (OTHER, RU, LAT) and right == OTHER:
        # Δσ mβж
        return JOIN


########
#
#  EXCEPTION
#
#######


def yahoo(split):
    if split.left_1.normal == 'yahoo' and split.right == '!':
        return JOIN


########
#
#   SPLIT
#
########


class Atom(Record):
    __attributes__ = ['start', 'stop', 'type', 'text']

    def __init__(self, start, stop, type, text):
        self.start = start
        self.stop = stop
        self.type = type
        self.text = text
        self.normal = text.lower()


class TokenSplit(Split):
    def __init__(self, left, delimiter, right):
        self.left_atoms = left
        self.right_atoms = right
        super(TokenSplit, self).__init__(
            self.left_1.text,
            delimiter,
            self.right_1.text
        )

    @cached_property
    def left_1(self):
        return self.left_atoms[-1]

    @cached_property
    def left_2(self):
        if len(self.left_atoms) > 1:
            return self.left_atoms[-2]

    @cached_property
    def left_3(self):
        if len(self.left_atoms) > 2:
            return self.left_atoms[-3]

    @cached_property
    def right_1(self):
        return self.right_atoms[0]

    @cached_property
    def right_2(self):
        if len(self.right_atoms) > 1:
            return self.right_atoms[1]

    @cached_property
    def right_3(self):
        if len(self.right_atoms) > 2:
            return self.right_atoms[2]


class TokenSplitter(Splitter):
    def __init__(self, window=3):
        self.window = window

    def atoms(self, text):
        matches = ATOM.finditer(text)
        for match in matches:
            start = match.start()
            stop = match.end()
            type = match.lastgroup
            text = match.group(0)
            yield Atom(
                start, stop,
                type, text
            )

    def __call__(self, text):
        atoms = list(self.atoms(text))
        for index in range(len(atoms)):
            atom = atoms[index]
            if index > 0:
                previous = atoms[index - 1]
                delimiter = text[previous.stop:atom.start]
                left = atoms[max(0, index - self.window):index]
                right = atoms[index:index + self.window]
                yield TokenSplit(left, delimiter, right)
            yield atom.text


########
#
#   SEGMENT
#
########


RULES = [
    DashRule(),
    UnderscoreRule(),
    FloatRule(),
    FractionRule(),

    FunctionRule(punct),
    FunctionRule(other),

    FunctionRule(yahoo),
]


class TokenSegmenter(Segmenter):
    def __init__(self):
        super(TokenSegmenter, self).__init__(TokenSplitter(), RULES)

    def segment(self, parts):
        buffer = safe_next(parts)
        if buffer is None:
            return

        for split in parts:
            right = next(parts)
            split.buffer = buffer
            if not split.delimiter and self.join(split):
                buffer += right
            else:
                yield buffer
                buffer = right
        yield buffer

    @property
    def debug(self):
        return DebugTokenSegmenter()


class DebugTokenSegmenter(TokenSegmenter, DebugSegmenter):
    pass


tokenize = TokenSegmenter()
