
import re

from razdel.record import cached_property
from razdel.rule import (
    JOIN,
    FunctionRule
)
from razdel.split import (
    Split,
    Splitter,
)

from .base import (
    Segmenter,
    DebugSegmenter
)
from .sokr import (
    HEAD_SOKRS,
    SOKRS,

    HEAD_PAIR_SOKRS,
    PAIR_SOKRS,

    INITIALS
)
from .punct import (
    ENDINGS,
    DASHES,

    QUOTES,
    CLOSE_QUOTES,
    GENERIC_QUOTES,
    CLOSE_BRACKETS,

    SMILES,
)


SPACE_SUFFIX = re.compile(r'\s$', re.U)
SPACE_PREFIX = re.compile(r'^\s', re.U)

TOKEN = re.compile(r'([^\W\d]+|\d+|[^\w\s])', re.U)
FIRST_TOKEN = re.compile(r'^\s*([^\W\d]+|\d+|[^\w\s])', re.U)
LAST_TOKEN = re.compile(r'([^\W\d]+|\d+|[^\w\s])\s*$', re.U)
WORD = re.compile(r'([^\W\d]+|\d+)', re.U)
PAIR_SOKR = re.compile(r'(\w)\s*\.\s*(\w)\s*$', re.U)
INT_SOKR = re.compile(r'\d+\s*-?\s*(\w+)\s*$', re.U)

ROMAN = re.compile(r'^[IVXML]+$', re.U)
BULLET_CHARS = set('§абвгдеabcdef')
BULLET_BOUNDS = '.)'
BULLET_SIZE = 20

DELIMITERS = ENDINGS + ';' + GENERIC_QUOTES + CLOSE_QUOTES + CLOSE_BRACKETS
SMILE_PREFIX = re.compile(r'^\s*' + SMILES, re.U)


def is_lower_alpha(token):
    return token.isalpha() and token.islower()


#######
#
#   TRIVIAL
#
######


def empty_side(split):
    if not split.left_token or not split.right_token:
        return JOIN


def no_space_prefix(split):
    if not split.right_space_prefix:
        return JOIN


def lower_right(split):
    if is_lower_alpha(split.right_token):
        return JOIN


def delimiter_right(split):
    right = split.right_token
    if right in GENERIC_QUOTES:
        return

    if right in DELIMITERS:
        return JOIN

    if SMILE_PREFIX.match(split.right):
        return JOIN


########
#
#   SOKR
#
#########


def is_sokr(token):
    if token.isdigit():
        return True
    if not token.isalpha():
        return True  # punct
    return token.islower()  # lower alpha


def sokr_left(split):
    if split.delimiter != '.':
        return

    right = split.right_token
    match = split.left_pair_sokr
    if match:
        a, b = match
        left = a.lower(), b.lower()

        if left in HEAD_PAIR_SOKRS:
            return JOIN

        if left in PAIR_SOKRS:
            if is_sokr(right):
                return JOIN
            return

    left = split.left_token.lower()
    if left in HEAD_SOKRS:
        return JOIN

    if left in SOKRS and is_sokr(right):
            return JOIN


def inside_pair_sokr(split):
    if split.delimiter != '.':
        return

    left = split.left_token.lower()
    right = split.right_token.lower()
    if (left, right) in PAIR_SOKRS:
        return JOIN


def initials_left(split):
    if split.delimiter != '.':
        return

    left = split.left_token
    if left.isupper() and len(left) == 1:
        return JOIN
    if left.lower() in INITIALS:
        return JOIN


##########
#
#  BOUND
#
########


def close_bound(split):
    left = split.left_token
    if left in ENDINGS:
        return
    return JOIN


def close_quote(split):
    delimiter = split.delimiter
    if delimiter not in QUOTES:
        return

    if delimiter in CLOSE_QUOTES:
        return close_bound(split)

    if delimiter in GENERIC_QUOTES:
        if not split.left_space_suffix:
            return close_bound(split)
        return JOIN


def close_bracket(split):
    if split.delimiter in CLOSE_BRACKETS:
        return close_bound(split)


#######
#
#   BULLET
#
########


def is_bullet(token):
    if token.isdigit():
        return True
    if token in BULLET_BOUNDS:
        # "8.1." "2)."
        return True
    if token.lower() in BULLET_CHARS:
        return True
    if ROMAN.match(token):
        return True


def list_item(split):
    if split.delimiter not in BULLET_BOUNDS:
        return

    if len(split.buffer) > BULLET_SIZE:
        return

    if all(is_bullet(_) for _ in split.buffer_tokens):
        return JOIN


#########
#
#   DASH
#
########


def dash_right(split):
    if split.right_token not in DASHES:
        return

    right = split.right_word
    if right and is_lower_alpha(right):
        return JOIN


##########
#
#   SPLIT
#
##########


class SentSplit(Split):
    @cached_property
    def right_space_prefix(self):
        return bool(SPACE_PREFIX.match(self.right))

    @cached_property
    def left_space_suffix(self):
        return bool(SPACE_SUFFIX.search(self.left))

    @cached_property
    def right_token(self):
        match = FIRST_TOKEN.match(self.right)
        if match:
            return match.group(1)

    @cached_property
    def left_token(self):
        match = LAST_TOKEN.search(self.left)
        if match:
            return match.group(1)

    @cached_property
    def left_pair_sokr(self):
        match = PAIR_SOKR.search(self.left)
        if match:
            return match.groups()

    @cached_property
    def left_int_sokr(self):
        match = INT_SOKR.search(self.left)
        if match:
            return match.group(1)

    @cached_property
    def right_word(self):
        match = WORD.search(self.right)
        if match:
            return match.group(1)

    @cached_property
    def buffer_tokens(self):
        return TOKEN.findall(self.buffer)

    @cached_property
    def buffer_first_token(self):
        match = FIRST_TOKEN.match(self.buffer)
        if match:
            return match.group(1)


DELIMITER = '({smiles}|[{delimiters}])'.format(
    delimiters=re.escape(DELIMITERS),
    smiles=SMILES
)


class SentSplitter(Splitter):
    __attributes__ = ['pattern', 'window']

    def __init__(self, pattern=DELIMITER, window=10):
        self.pattern = pattern
        self.window = window
        self.re = re.compile(pattern, re.U)

    def __call__(self, text):
        matches = self.re.finditer(text)
        previous = 0
        for match in matches:
            start = match.start()
            stop = match.end()
            delimiter = match.group(1)
            yield text[previous:start]
            left = text[max(0, start - self.window):start]
            right = text[stop:stop + self.window]
            yield SentSplit(left, delimiter, right)
            previous = stop
        yield text[previous:]


########
#
#   SEGMENT
#
########


RULES = [FunctionRule(_) for _ in [
    empty_side,
    no_space_prefix,
    lower_right,
    delimiter_right,

    sokr_left,
    inside_pair_sokr,
    initials_left,

    list_item,

    close_quote,
    close_bracket,

    dash_right,
]]


class SentSegmenter(Segmenter):
    def __init__(self, split=SentSplitter(), rules=RULES):
        super(SentSegmenter, self).__init__(split, rules)

    @property
    def debug(self):
        return DebugSentSegmenter()

    def post(self, chunks):
        for chunk in chunks:
            yield chunk.strip()


class DebugSentSegmenter(SentSegmenter, DebugSegmenter):
    pass


sentenize = SentSegmenter()
