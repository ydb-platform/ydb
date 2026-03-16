# A text parser.

import re
from collections import namedtuple
from operator import itemgetter


__author__ = 'Erik Moqvist'
__version__ = '0.24.0'


class _Mismatch(object):
    pass


MISMATCH = _Mismatch()
"""Returned by :func:`~textparser.Pattern.match()` on mismatch.

"""


class _String(object):
    """Matches a specific token kind.

    """

    def __init__(self, kind):
        self.kind = kind

    def match(self, tokens):
        if self.kind == tokens.peek().kind:
            return tokens.get_value()
        else:
            return MISMATCH


class _Tokens(object):

    def __init__(self, tokens):
        self._tokens = tokens
        self._pos = 0
        self._max_pos = -1
        self._stack = []

    def get_value(self):
        pos = self._pos
        self._pos += 1

        return self._tokens[pos]

    def peek(self):
        return self._tokens[self._pos]

    def peek_max(self):
        pos = self._pos

        if self._max_pos > pos:
            pos = self._max_pos

        if pos >= len(self._tokens):
            return self._tokens[-1]
        else:
            return self._tokens[pos]

    def save(self):
        self._stack.append(self._pos)

    def restore(self):
        self._pos = self._stack.pop()

    def update(self):
        self._stack[-1] = self._pos

    def mark_max_restore(self):
        if self._pos > self._max_pos:
            self._max_pos = self._pos

        self._pos = self._stack.pop()

    def mark_max_load(self):
        if self._pos > self._max_pos:
            self._max_pos = self._pos

        self._pos = self._stack[-1]

    def drop(self):
        self._stack.pop()

    def __repr__(self):
        return str(self._tokens[self._pos:self._pos + 2])


class _StringTokens(_Tokens):

    def get_value(self):
        pos = self._pos
        self._pos += 1

        return self._tokens[pos].value


def _wrap_string(item):
    if isinstance(item, str):
        item = _String(item)

    return item


def _wrap_strings(items):
    return [_wrap_string(item) for item in items]


def _format_invalid_syntax(text, offset):
    return 'Invalid syntax at line {}, column {}: "{}"'.format(
        line(text, offset),
        column(text, offset),
        markup_line(text, offset))


class Error(Exception):
    """General textparser exception.

    """

    pass


class TokenizeError(Error):
    """This exception is raised when the text cannot be converted into
    tokens.

    """

    def __init__(self, text, offset):
        self._text = text
        self._offset = offset
        message = _format_invalid_syntax(text, offset)
        super(TokenizeError, self).__init__(message)

    @property
    def text(self):
        """The input text to the tokenizer.

        """

        return self._text

    @property
    def offset(self):
        """Offset into the text where the tokenizer failed.

        """

        return self._offset


class GrammarError(Error):
    """This exception is raised when the tokens cannot be converted into a
    parse tree.

    """

    def __init__(self, offset):
        self._offset = offset
        message = 'Invalid syntax at offset {}.'.format(offset)
        super(GrammarError, self).__init__(message)

    @property
    def offset(self):
        """Offset into the text where the parser failed.

        """

        return self._offset


class ParseError(Error):
    """This exception is raised when the parser fails to parse the text.

    """

    def __init__(self, text, offset):
        self._text = text
        self._offset = offset
        self._line = line(text, offset)
        self._column = column(text, offset)
        message = _format_invalid_syntax(text, offset)
        super(ParseError, self).__init__(message)

    @property
    def text(self):
        """The input text to the parser.

        """

        return self._text

    @property
    def offset(self):
        """Offset into the text where the parser failed.

        """

        return self._offset

    @property
    def line(self):
        """Line where the parser failed.

        """

        return self._line

    @property
    def column(self):
        """Column where the parser failed.

        """

        return self._column


Token = namedtuple('Token', ['kind', 'value', 'offset'])


class Pattern(object):
    """Base class of all patterns.

    """

    def match(self, tokens):
        """Returns :data:`~textparser.MISMATCH` on mismatch, and anything else
        on match.

        """

        raise NotImplementedError('To be implemented by subclasses.')


class Sequence(Pattern):
    """Matches a sequence of patterns. Becomes a list in the parse tree.

    """

    def __init__(self, *patterns):
        self.patterns = _wrap_strings(patterns)

    def match(self, tokens):
        matched = []

        for pattern in self.patterns:
            mo = pattern.match(tokens)

            if mo is MISMATCH:
                return MISMATCH

            matched.append(mo)

        return matched


class Choice(Pattern):
    """Matches any of given ordered patterns `patterns`. The first pattern
    in the list has highest priority, and the last lowest.

    """

    def __init__(self, *patterns):
        self._patterns = _wrap_strings(patterns)

    def match(self, tokens):
        tokens.save()

        for pattern in self._patterns:
            tokens.mark_max_load()
            mo = pattern.match(tokens)

            if mo is not MISMATCH:
                tokens.drop()

                return mo

        tokens.restore()

        return MISMATCH


class ChoiceDict(Pattern):
    """Matches any of given patterns. The first token kind of all patterns
    must be unique, otherwise and :class:`~textparser.Error` exception
    is raised.

    This class is faster than :class:`~textparser.Choice`, and should
    be used if the grammar allows it.

    """

    def __init__(self, *patterns):
        self._patterns_map = {}
        patterns = _wrap_strings(patterns)

        for pattern in patterns:
            self._check_pattern(pattern, pattern)

    @property
    def patterns_map(self):
        return self._patterns_map

    def _check_pattern(self, inner, outer):
        if isinstance(inner, _String):
            self._add_pattern(inner.kind, outer)
        elif isinstance(inner, Sequence):
            self._check_pattern(inner.patterns[0], outer)
        elif isinstance(inner, (Tag, Forward)):
            self._check_pattern(inner.pattern, outer)
        elif isinstance(inner, ChoiceDict):
            for pattern in inner.patterns_map.values():
                self._check_pattern(pattern, outer)
        else:
            raise Error(
                'Unsupported pattern type {}.'.format(type(inner)))

    def _add_pattern(self, kind, pattern):
        if kind in self._patterns_map:
            raise Error(
                "First token kind must be unique, but {} isn't.".format(
                    kind))

        self._patterns_map[kind] = pattern

    def match(self, tokens):
        kind = tokens.peek().kind

        if kind in self._patterns_map:
            return self._patterns_map[kind].match(tokens)
        else:
            return MISMATCH


class Repeated(Pattern):
    """Matches `pattern` at least `minimum` times. Any match becomes a
    list in the parse tree.

    """

    def __init__(self, pattern, minimum=0):
        self._pattern = _wrap_string(pattern)
        self._minimum = minimum

    def match(self, tokens):
        matched = []
        tokens.save()

        while True:
            mo = self._pattern.match(tokens)

            if mo is MISMATCH:
                tokens.mark_max_restore()
                break

            matched.append(mo)
            tokens.update()

        if len(matched) >= self._minimum:
            return matched
        else:
            return MISMATCH


class RepeatedDict(Repeated):
    """Same as :class:`~textparser.Repeated`, but becomes a dictionary
    instead of a list in the parse tree.

    `key` is a function taking the match as input and returning the
    dictionary key. By default the first element in the match is used
    as key.

    """

    def __init__(self, pattern, minimum=0, key=None):
        super(RepeatedDict, self).__init__(pattern, minimum)

        if key is None:
            key = itemgetter(0)

        self._key = key

    def match(self, tokens):
        matched = {}
        tokens.save()

        while True:
            mo = self._pattern.match(tokens)

            if mo is MISMATCH:
                tokens.mark_max_restore()
                break

            key = self._key(mo)

            try:
                matched[key].append(mo)
            except KeyError:
                matched[key] = [mo]

            tokens.update()

        if len(matched) >= self._minimum:
            return matched
        else:
            return MISMATCH


class ZeroOrMore(Repeated):
    """Matches `pattern` zero or more times.

    See :class:`~textparser.Repeated` for more details.

    """

    def __init__(self, pattern):
        super(ZeroOrMore, self).__init__(pattern, 0)


class ZeroOrMoreDict(RepeatedDict):
    """Matches `pattern` zero or more times.

    See :class:`~textparser.RepeatedDict` for more details.

    """

    def __init__(self, pattern, key=None):
        super(ZeroOrMoreDict, self).__init__(pattern, 0, key)


class OneOrMore(Repeated):
    """Matches `pattern` one or more times.

    See :class:`~textparser.Repeated` for more details.

    """

    def __init__(self, pattern):
        super(OneOrMore, self).__init__(pattern, 1)


class OneOrMoreDict(RepeatedDict):
    """Matches `pattern` one or more times.

    See :class:`~textparser.RepeatedDict` for more details.

    """

    def __init__(self, pattern, key=None):
        super(OneOrMoreDict, self).__init__(pattern, 1, key)


class DelimitedList(Pattern):
    """Matches a delimented list of `pattern` separated by
    `delim`. `pattern` must be matched at least once. Any match
    becomes a list in the parse tree, excluding the delimiters.

    """

    def __init__(self, pattern, delim=','):
        self._pattern = _wrap_string(pattern)
        self._delim = _wrap_string(delim)

    def match(self, tokens):
        # First pattern.
        mo = self._pattern.match(tokens)

        if mo is MISMATCH:
            return MISMATCH

        matched = [mo]
        tokens.save()

        while True:
            # Discard the delimiter.
            mo = self._delim.match(tokens)

            if mo is MISMATCH:
                break

            # Pattern.
            mo = self._pattern.match(tokens)

            if mo is MISMATCH:
                break

            matched.append(mo)
            tokens.update()

        tokens.restore()

        return matched


class Optional(Pattern):
    """Matches `pattern` zero or one times. Becomes a list in the parse
    tree, empty on mismatch.

    """

    def __init__(self, pattern):
        self._pattern = _wrap_string(pattern)

    def match(self, tokens):
        tokens.save()
        mo = self._pattern.match(tokens)

        if mo is MISMATCH:
            tokens.mark_max_restore()

            return []
        else:
            tokens.drop()

            return [mo]


class Any(Pattern):
    """Matches any token.

    """

    def match(self, tokens):
        if tokens.peek().kind == '__EOF__':
            return MISMATCH
        else:
            return tokens.get_value()


class AnyUntil(Pattern):
    """Matches any token until given pattern is found. Becomes a list in
    the parse tree, not including the given pattern match.

    """

    def __init__(self, pattern):
        self._pattern = _wrap_string(pattern)

    def match(self, tokens):
        matched = []

        while True:
            tokens.save()
            mo = self._pattern.match(tokens)

            if mo is not MISMATCH:
                break

            tokens.restore()
            matched.append(tokens.get_value())

        tokens.restore()

        return matched


class And(Pattern):
    """Matches `pattern`, without consuming any tokens. Any match becomes
    an empty list in the parse tree.

    """

    def __init__(self, pattern):
        self._pattern = _wrap_string(pattern)

    def match(self, tokens):
        tokens.save()
        mo = self._pattern.match(tokens)
        tokens.restore()

        if mo is MISMATCH:
            return MISMATCH
        else:
            return []


class Not(Pattern):
    """Matches if `pattern` does not match. Any match becomes an empty
    list in the parse tree.

    Just like :class:`~textparser.And`, no tokens are consumed.

    """

    def __init__(self, pattern):
        self._pattern = _wrap_string(pattern)

    def match(self, tokens):
        tokens.save()
        mo = self._pattern.match(tokens)
        tokens.restore()

        if mo is MISMATCH:
            return []
        else:
            return MISMATCH


class NoMatch(Pattern):
    """Never matches anything.

    """

    def match(self, tokens):
        return MISMATCH


class Tag(Pattern):
    """Tags any matched `pattern` with name `name`. Becomes a two-tuple of
    `name` and match in the parse tree.

    """

    def __init__(self, name, pattern):
        self._name = name
        self._pattern = _wrap_string(pattern)

    @property
    def pattern(self):
        return self._pattern

    def match(self, tokens):
        mo = self._pattern.match(tokens)

        if mo is not MISMATCH:
            return (self._name, mo)
        else:
            return MISMATCH


class Forward(Pattern):
    """Forward declaration of a pattern.

    .. code-block:: python

       >>> foo = Forward()
       >>> foo <<= Sequence('NUMBER')

    """

    def __init__(self):
        self._pattern = None

    @property
    def pattern(self):
        return self._pattern

    def __ilshift__(self, other):
        self._pattern = _wrap_string(other)

        return self

    def match(self, tokens):
        return self._pattern.match(tokens)


class Grammar(object):
    """Creates a tree of given tokens using the grammar `grammar`.

    """

    def __init__(self, grammar):
        if isinstance(grammar, str):
            grammar = _wrap_string(grammar)

        self._root = grammar

    def parse(self, tokens, token_tree=False):
        if token_tree:
            tokens = _Tokens(tokens)
        else:
            tokens = _StringTokens(tokens)

        parsed = self._root.match(tokens)

        if parsed is not MISMATCH and tokens.peek_max().kind == '__EOF__':
            return parsed
        else:
            raise GrammarError(tokens.peek_max().offset)


def choice(*patterns):
    """Returns an instance of the fastest choice class for given patterns
    `patterns`. It is recommended to use this function instead of
    instantiate :class:`~textparser.Choice` or
    :class:`~textparser.ChoiceDict` directly.

    """

    try:
        return ChoiceDict(*patterns)
    except Error:
        return Choice(*patterns)


def markup_line(text, offset, marker='>>!<<'):
    """Insert `marker` at `offset` into `text`, and return the marked
    line.

    .. code-block:: python

       >>> markup_line('0\\n1234\\n56', 3)
       1>>!<<234

    """

    begin = text.rfind('\n', 0, offset)
    begin += 1

    end = text.find('\n', offset)

    if end == -1:
        end = len(text)

    return text[begin:offset] + marker + text[offset:end]


def line(text, offset):
    return text[:offset].count('\n') + 1


def column(text, offset):
    line_start = text.rfind('\n', 0, offset)

    return offset - line_start


def tokenize_init(spec):
    """Initialize a tokenizer. Should only be called by the
    :func:`~textparser.Parser.tokenize` method in the parser.

    """

    tokens = [Token('__SOF__', '__SOF__', 0)]
    re_token = '|'.join([
        '(?P<{}>{})'.format(name, regex) for name, regex in spec
    ])

    return tokens, re_token


class Parser(object):
    """The abstract base class of all text parsers.

    .. code-block:: python

       >>> from textparser import Parser, Sequence
       >>> class MyParser(Parser):
       ...    def token_specs(self):
       ...        return [
       ...            ('SKIP',          r'[ \\r\\n\\t]+'),
       ...            ('WORD',          r'\\w+'),
       ...            ('EMARK',    '!', r'!'),
       ...            ('COMMA',    ',', r','),
       ...            ('MISMATCH',      r'.')
       ...        ]
       ...    def grammar(self):
       ...        return Sequence('WORD', ',', 'WORD', '!')

    """

    def _unpack_token_specs(self):
        names = {}
        specs = []

        for spec in self.token_specs():
            if len(spec) == 2:
                specs.append(spec)
            else:
                specs.append((spec[0], spec[2]))
                names[spec[0]] = spec[1]

        return names, specs

    def keywords(self):
        """A set of keywords in the text.

        .. code-block:: python

           def keywords(self):
               return set(['if', 'else'])

        """

        return set()

    def token_specs(self):
        """The token specifications with token name, regular expression, and
        optionally a user friendly name.

        Two token specification forms are available; ``(kind, re)`` or
        ``(kind, name, re)``. If the second form is used, the grammar
        should use `name` instead of `kind`.

        See :class:`~textparser.Parser` for an example usage.

        """

        return [
            ('SKIP',                r'[ \r\n\t]+'),
            ('NUMBER',              r'-?\d+(\.\d+)?([eE][+-]?\d+)?'),
            ('WORD',                r'[A-Za-z0-9_]+'),
            ('ESCAPED_STRING',      r'"(\\"|[^"])*?"'),
            ('MISMATCH',            r'.')
        ]

    def tokenize(self, text):
        """Tokenize given string `text`, and return a list of tokens. Raises
        :class:`~textparser.TokenizeError` on failure.

        This method should only be called by
        :func:`~textparser.Parser.parse()`, but may very well be
        overridden if the default implementation does not match the
        parser needs.

        """

        names, specs = self._unpack_token_specs()
        keywords = self.keywords()
        tokens, re_token = tokenize_init(specs)

        for mo in re.finditer(re_token, text, re.DOTALL):
            kind = mo.lastgroup

            if kind == 'SKIP':
                pass
            elif kind != 'MISMATCH':
                value = mo.group(kind)

                if value in keywords:
                    kind = value

                if kind in names:
                    kind = names[kind]

                tokens.append(Token(kind, value, mo.start()))
            else:
                raise TokenizeError(text, mo.start())

        return tokens

    def grammar(self):
        """The text grammar is used to create a parse tree out of a list of
        tokens.

        See :class:`~textparser.Parser` for an example usage.

        """

        raise NotImplementedError('No grammar defined.')

    def parse(self, text, token_tree=False, match_sof=False):
        """Parse given string `text` and return the parse tree. Raises
        :class:`~textparser.ParseError` on failure.

        Returns a parse tree of tokens if `token_tree` is ``True``.

        .. code-block:: python

           >>> MyParser().parse('Hello, World!')
           ['Hello', ',', 'World', '!']
           >>> tree = MyParser().parse('Hello, World!', token_tree=True)
           >>> from pprint import pprint
           >>> pprint(tree)
           [Token(kind='WORD', value='Hello', offset=0),
            Token(kind=',', value=',', offset=5),
            Token(kind='WORD', value='World', offset=7),
            Token(kind='!', value='!', offset=12)]

        """

        try:
            tokens = self.tokenize(text)

            if len(tokens) == 0 or tokens[-1].kind != '__EOF__':
                tokens.append(Token('__EOF__', '__EOF__', len(text)))

            if not match_sof:
                if len(tokens) > 0 and tokens[0].kind == '__SOF__':
                    del tokens[0]

            return Grammar(self.grammar()).parse(tokens, token_tree)
        except (TokenizeError, GrammarError) as e:
            raise ParseError(text, e.offset)


def replace_blocks(string, start='{', end='}'):
    """Replace all blocks starting with `start` and ending with `end` with
    spaces (not including `start` and `end`).

    """

    chunks = []
    begin = 0
    depth = 0
    start_length = len(start)
    pattern = r'({}|{})'.format(re.escape(start), re.escape(end))

    for mo in re.finditer(pattern, string):
        pos = mo.start()

        if mo.group() == start:
            if depth == 0:
                chunks.append(string[begin:pos + start_length])
                begin = (pos + start_length)

            depth += 1
        elif depth > 0:
            depth -= 1

            if depth == 0:
                for chunk in string[begin:pos].split('\n'):
                    chunks.append(' ' * len(chunk))
                    chunks.append('\n')

                chunks.pop()
                begin = pos

    chunks.append(string[begin:])

    return ''.join(chunks)
