#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
This module contains base classes and helper functions for defining Pratt parsers.
"""
import sys
import re
from abc import ABCMeta
from collections.abc import Callable, Iterator, MutableMapping, MutableSequence
from decimal import Decimal, DecimalException
from typing import Any, cast, overload, Generic, TypeVar
from unicodedata import name as unicode_name

#
# Simple top-down parser based on Vaughan Pratt's algorithm (Top Down Operator Precedence).
#
# References:
#
#   https://tdop.github.io/  (Vaughan R. Pratt's "Top Down Operator Precedence" - 1973)
#   http://crockford.com/javascript/tdop/tdop.html  (Douglas Crockford - 2007)
#   http://effbot.org/zone/simple-top-down-parsing.htm (Fredrik Lundh - 2008)
#
# This implementation is based on a base class for tokens and a base class for parsers.
# A real parser is built with a derivation of the base parser class followed by the
# registrations of token classes for the symbols of the language.
#
# A parser can be extended by derivation, copying the reusable token classes and
# defining the additional ones. See the files xpath1_parser.py and xpath2_parser.py
# for a full implementation example of a real parser.
#

# Parser special symbols set, that includes the special symbols of TDOP plus two
# additional special symbols for managing invalid literals and unknown symbols
# and source start.
SPECIAL_SYMBOLS = frozenset((
    '(start)', '(end)', '(string)', '(float)', '(decimal)',
    '(integer)', '(name)', '(invalid)', '(unknown)',
))


class ParseError(SyntaxError):
    """An error when parsing source with TDOP parser."""


def symbol_to_classname(symbol: str) -> str:
    """
    Converts a symbol string to an identifier (only alphanumeric and '_').
    """
    def get_id_name(c: str) -> str:
        if c.isalnum() or c == '_':
            return c
        else:
            return '%s_' % unicode_name(str(c)).title()

    if symbol.isalnum():
        return symbol.title()
    elif symbol in SPECIAL_SYMBOLS:
        return symbol[1:-1].title()
    elif all(c in '-_' for c in symbol):
        value = ' '.join(unicode_name(c) for c in symbol)
        return value.title().replace(' ', '').replace('-', '').replace('_', '')

    value = symbol.replace('-', '_')
    if value.isidentifier():
        return value.title().replace('_', '')

    value = ''.join(get_id_name(c) for c in symbol)
    return value.replace(' ', '').replace('-', '').replace('_', '')


class MultiLabel:
    """
    Helper class for defining multi-value label for tokens. Useful when a symbol
    has more roles. A label of this type has equivalence with each of its values.

    Example:
        label = MultiLabel('function', 'operator')
        label == 'symbol'    # False
        label == 'function'  # True
        label == 'operator'  # True
    """
    def __init__(self, *values: str) -> None:
        self.values = values

    def __eq__(self, other: object) -> bool:
        return any(other == v for v in self.values)

    def __ne__(self, other: object) -> bool:
        return all(other != v for v in self.values)

    def __repr__(self) -> str:
        return '%s%s' % (self.__class__.__name__, self.values)

    def __str__(self) -> str:
        return '__'.join(self.values).replace(' ', '_')

    def __hash__(self) -> int:
        return hash(self.values)

    def __contains__(self, item: str) -> bool:
        return any(item in v for v in self.values)

    def startswith(self, s: str) -> bool:
        return any(v.startswith(s) for v in self.values)

    def endswith(self, s: str) -> bool:
        return any(v.endswith(s) for v in self.values)


TK = TypeVar('TK', bound='Token[Any]')


class Token(MutableSequence[TK]):
    """
    Token base class for defining a parser based on Pratt's method.

    Each token instance is a list-like object. The number of token's items is
    the arity of the represented operator, where token's items are the operands.
    Nullary operators are used for symbols, names and literals. Tokens with items
    represent the other operators (unary, binary and so on).

    Each token class has a *symbol*, a lbp (left binding power) value and a rbp
    (right binding power) value, that are used in the sense described by the
    Pratt's method. This implementation of Pratt tokens includes two extra
    attributes, *pattern* and *label*, that can be used to simplify the parsing
    of symbols in a concrete parser.

    :param parser: The parser instance that creates the token instance.
    :param value: The token value. If not provided defaults to token symbol.

    :cvar symbol: the symbol of the token class.
    :cvar lbp: Pratt's left binding power, defaults to 0.
    :cvar rbp: Pratt's right binding power, defaults to 0.
    :cvar pattern: the regex pattern used for the token class. Defaults to the \
    escaped symbol. Can be customized to match more detailed conditions (e.g. a \
    function with its left round bracket), in order to simplify the related code.
    :cvar label: defines the typology of the token class. Its value is used in \
    representations of the token instance and can be used to restrict code choices \
    without more complicated analysis. The label value can be set as needed by the \
    parser implementation (e.g. 'function', 'axis', 'constructor function' are used by \
    the XPath parsers). In the base parser class defaults to 'symbol' with 'literal' \
    and 'operator' as possible alternatives. If set by a tuple of values the token \
    class label is transformed to a multi-value label, that means the token class can \
    covers multiple roles (e.g. as XPath function or axis). In those cases the definitive \
    role is defined at parse time (nud and/or led methods) after the token instance creation.
    """
    lbp: int = 0           # left binding power
    rbp: int = 0           # right binding power
    symbol: str = ''       # the token identifier
    lookup_name: str = ''  # the key in symbol table, usually matches the symbol.
    label: str | MultiLabel = 'symbol'  # the label, that usually means a group of token types.
    pattern: str | None = None  # a custom regex pattern for building the tokenizer

    __slots__ = '_items', 'parser', 'value', 'span', '__dict__'

    _items: list[TK]
    parser: 'Parser[TK]'
    value: Any
    span: tuple[int, int]

    def __init__(self, parser: 'Parser[TK]', value: Any | None = None) -> None:
        self._items = []
        self.parser = parser
        self.value = value if value is not None else self.symbol
        self.span = (0, 0) if parser.next_match is None else parser.next_match.span()

    @overload
    def __getitem__(self, i: int) -> TK: ...  # pragma: no cover

    @overload
    def __getitem__(self, s: slice) -> MutableSequence[TK]: ...  # pragma: no cover

    def __getitem__(self, i: int | slice) -> TK | MutableSequence[TK]:
        return self._items[i]

    def __setitem__(self, i: int | slice, o: Any) -> None:
        self._items[i] = o

    def __delitem__(self, i: int | slice) -> None:
        del self._items[i]

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[TK]:
        return iter(self._items)

    def insert(self, i: int, item: TK) -> None:
        self._items.insert(i, item)

    def __str__(self) -> str:
        if self.symbol in SPECIAL_SYMBOLS:
            return '%r %s' % (self.value, self.symbol[1:-1])
        else:
            return '%r %s' % (self.symbol, str(self.label))

    def __repr__(self) -> str:
        return '<%s object at %#x>' % (self.__class__.__name__, id(self))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Token):
            return self.symbol == other.symbol and self.value == other.value
        return False

    @property
    def arity(self) -> int:
        return len(self)

    @property
    def tree(self) -> str:
        """Returns a tree representation string."""
        if self.symbol == '(name)':
            return '(%s)' % self.value
        elif self.symbol in SPECIAL_SYMBOLS:
            return '(%r)' % self.value
        elif self.symbol == '(':
            if len(self) == 1:
                return self[0].tree
            return f"({' '.join(item.tree for item in self)})"
        elif not self:
            return '(%s)' % self.symbol
        else:
            return f"({self.symbol} {' '.join(item.tree for item in self)})"

    @property
    def source(self) -> str:
        """Returns the source representation string."""
        symbol = self.symbol
        if symbol == '(name)':
            return cast(str, self.value)
        elif symbol == '(decimal)':
            return str(self.value)
        elif symbol in SPECIAL_SYMBOLS:
            return repr(self.value).replace(r'\\', '\\')
        else:
            length = len(self)
            if not length:
                return symbol
            elif length == 1:
                if 'postfix' in self.label:
                    return '%s %s' % (self[0].source, symbol)
                return '%s %s' % (symbol, self[0].source)
            elif length == 2:
                return '%s %s %s' % (self[0].source, symbol, self[1].source)
            else:
                return '%s %s' % (symbol, ' '.join(item.source for item in self))

    @property
    def position(self) -> tuple[int, int]:
        """A tuple with the position of the token in terms of line and column."""
        token_index = self.span[0]
        line = self.parser.source[:token_index].count('\n') + 1
        if line == 1:
            return 1, token_index + 1
        return line, token_index - self.parser.source[:token_index].rindex('\n')

    def as_name(self) -> TK:
        """Returns a new '(name)' token for resolving ambiguous states."""
        assert self.parser.name_pattern.match(self.symbol) is not None, \
            "Token symbol is not compatible with the name pattern!"

        token = self.parser.symbol_table['(name)'](self.parser, self.symbol)
        token.span = self.span
        return token

    def is_source_start(self) -> bool:
        """
        Returns `True` if the token is positioned at the start
        of the source, ignoring the spaces.
        """
        return not bool(self.parser.source[0:self.span[0]].strip())

    def is_line_start(self) -> bool:
        """
        Returns `True` if the token is positioned at the start
        of a source line, ignoring the spaces.
        """
        token_index = self.span[0]
        try:
            line_start = self.parser.source[:token_index].rindex('\n') + 1
        except ValueError:
            return not bool(self.parser.source[:token_index].strip())
        else:
            return not bool(self.parser.source[line_start:token_index].strip())

    def is_spaced(self, before: bool = True, after: bool = True) -> bool:
        """
        Returns `True` if the token has extra spaces (whitespace, tab or newline)
        immediately before or after it.

        :param before: if `True` considers also the extra spaces before the token.
        :param after: if `True` considers also the extra spaces after the token.
        """
        start, end = self.span
        try:
            if before and start > 0 and self.parser.source[start - 1] in ' \t\n':
                return True
            return after and self.parser.source[end] in ' \t\n'
        except IndexError:
            return False

    def nud(self) -> TK:
        """Pratt's null denotation method"""
        raise self.wrong_syntax()

    def led(self, left: TK) -> TK:
        """Pratt's left denotation method"""
        raise self.wrong_syntax()

    def evaluate(self) -> Any:
        """Evaluation method"""
        return self.value

    def iter(self: TK, *symbols: str) -> Iterator[TK]:
        """Returns a generator for iterating the token's tree."""
        status: list[tuple[TK | None, Iterator[TK]]] = []
        parent: TK | None = self
        children: Iterator[TK] = iter(self)
        tk: TK

        while True:
            for tk in children:
                if parent is not None and len(parent._items) == 1:
                    if not symbols or parent.symbol in symbols:
                        yield parent
                    parent = None

                if not tk._items:
                    if not symbols or tk.symbol in symbols:
                        yield tk
                    if parent is not None:
                        if not symbols or parent.symbol in symbols:
                            yield parent
                        parent = None
                    continue
                status.append((parent, children))
                parent, children = tk, iter(tk)
                break
            else:
                try:
                    parent, children = status.pop()
                except IndexError:
                    if parent is not None:
                        if not symbols or parent.symbol in symbols:
                            yield parent
                    return
                else:
                    if parent is not None:
                        if not symbols or parent.symbol in symbols:
                            yield parent
                        parent = None

    def expected(self, *symbols: str, message: str | None = None) -> None:
        if symbols and self.symbol not in symbols:
            raise self.wrong_syntax(message)

    def unexpected(self, *symbols: str, message: str | None = None) -> None:
        if not symbols or self.symbol in symbols:
            raise self.wrong_syntax(message)

    def wrong_syntax(self, message: str | None = None) -> ParseError:
        if message:
            return ParseError(message)
        elif self.symbol not in SPECIAL_SYMBOLS:
            return ParseError('unexpected %s' % self)
        elif self.symbol == '(invalid)':
            return ParseError('invalid literal %r' % self.value)
        elif self.symbol == '(unknown)':
            return ParseError('unknown symbol %r' % self.value)
        elif self.symbol == '(name)':
            return ParseError('unexpected name %r' % self.value)
        elif self.symbol != '(end)':
            return ParseError('unexpected literal %r' % self.value)
        elif self.parser.token.symbol == '(start)':
            return ParseError('source is empty')
        else:
            return ParseError('unexpected end of source')

    def wrong_type(self, message: str = 'invalid type') -> TypeError:
        return TypeError(message)

    def wrong_value(self, message: str = 'invalid value') -> ValueError:
        return ValueError(message)


class ParserMeta(ABCMeta):

    token_base_class: type[Any]
    literals_pattern: re.Pattern[str]
    name_pattern: re.Pattern[str]
    tokenizer: re.Pattern[str] | None
    symbol_table: MutableMapping[str, type[Any]]

    def __new__(mcs, name: str, bases: tuple[type[Any], ...], namespace: dict[str, Any]) \
            -> 'ParserMeta':
        cls = super(ParserMeta, mcs).__new__(mcs, name, bases, namespace)

        # Avoids more parsers definitions for a single module
        for k, v in sys.modules[cls.__module__].__dict__.items():
            if isinstance(v, ParserMeta) and v.__module__ == cls.__module__:
                raise RuntimeError("Multiple parser class definitions per module are not allowed")

        # Checks and initializes class attributes
        if not hasattr(cls, 'token_base_class'):
            cls.token_base_class = Token
        if not hasattr(cls, 'literals_pattern'):
            cls.literals_pattern = re.compile(
                r"""'[^']*'|"[^"]*"|(?:\d+|\.\d+)(?:\.\d*)?(?:[Ee][+-]?\d+)?"""
            )
        if not hasattr(cls, 'name_pattern'):
            cls.name_pattern = re.compile(r'[A-Za-z0-9_]+')
        if 'tokenizer' not in namespace:
            cls.tokenizer = None
        if 'symbol_table' not in namespace:
            cls.symbol_table = {}
            for base_class in bases:
                if hasattr(base_class, 'symbol_table'):
                    cls.symbol_table.update(base_class.symbol_table)
                    break
        return cls


TK_co = TypeVar('TK_co', bound=Token[Any], covariant=True)

RT = TypeVar('RT')


class Parser(Generic[TK_co], metaclass=ParserMeta):
    """
    Parser class for implementing a Top-Down Operator Precedence parser.

    :cvar   symbol_table: a dictionary that stores the token classes defined for the language.
    :cvar token_base_class: the base class for creating language's token classes.
    :cvar tokenizer: the language tokenizer compiled regexp.
    """
    token_base_class = Token
    tokenizer: re.Pattern[str] | None = None
    symbol_table: dict[str, type[TK_co]] = {}

    _start_token: TK_co
    source: str
    tokens: Iterator[re.Match[str]]
    token: TK_co
    next_token: TK_co
    next_match: re.Match[str] | None
    literals_pattern: re.Pattern[str]
    name_pattern: re.Pattern[str]

    __slots__ = 'source', 'tokens', 'next_match', '_start_token', 'token', 'next_token'

    def __init__(self) -> None:
        if self.tokenizer is None:
            self.build()
        self.source = ''
        self.tokens = iter(())
        self.next_match = None
        self._start_token = self.symbol_table['(start)'](self)
        self.token = self.next_token = self._start_token

    def __repr__(self) -> str:
        return '<%s object at %#x>' % (self.__class__.__name__, id(self))

    def __str__(self) -> str:
        return f'{self.__class__.__name__}()'

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Parser) and \
            self.token_base_class is other.token_base_class and \
            self.symbol_table == other.symbol_table

    def parse(self, source: str) -> TK_co:
        """
        Parses a source code of the formal language. This is the main method that has to be
        called for a parser's instance.

        :param source: The source string.
        :return: The root of the token's tree that parse the source.
        """
        assert self.tokenizer, "Parser tokenizer is not built!"
        try:
            try:
                self.tokens = iter(self.tokenizer.finditer(source))
            except TypeError as err:
                token = self.symbol_table['(invalid)'](self, source)
                raise token.wrong_syntax('invalid source type, {}'.format(err))

            self.source = source
            self.advance()
            root_token = self.expression()
            self.next_token.expected('(end)')
            return root_token
        finally:
            self.tokens = iter(())
            self.next_match = None
            self.token = self.next_token = self._start_token

    def advance(self, *symbols: str, message: str | None = None) -> TK_co:
        """
        The Pratt's function for advancing to next token.

        :param symbols: Optional arguments tuple. If not empty one of the provided \
        symbols is expected. If the next token's symbol differs the parser raises a \
        parse error.
        :param message: Optional custom message for unexpected symbols.
        :return: The current token instance.
        """
        value: Any
        if self.next_token.symbol == '(end)':
            raise self.next_token.wrong_syntax()
        elif symbols and self.next_token.symbol not in symbols:
            raise self.next_token.wrong_syntax(message)

        self.token = self.next_token

        for self.next_match in self.tokens:
            assert self.next_match is not None
            if not self.next_match.group().isspace():
                break
        else:
            self.next_token = self.symbol_table['(end)'](self)
            return self.token

        literal, symbol, name, unknown = self.next_match.groups()
        if symbol is not None:
            if symbol in self.symbol_table:
                self.next_token = self.symbol_table[symbol](self)
            elif self.name_pattern.match(symbol) is not None:
                self.next_token = self.symbol_table['(name)'](self, symbol)
            else:
                self.next_token = self.symbol_table['(unknown)'](self, symbol)
                raise self.next_token.wrong_syntax()

        elif literal is not None:
            if literal[0] in '\'"':
                value = self.unescape(literal)
                self.next_token = self.symbol_table['(string)'](self, value)
            elif 'e' in literal or 'E' in literal:
                try:
                    value = float(literal)
                except ValueError as err:
                    self.next_token = self.symbol_table['(invalid)'](self, literal)
                    raise self.next_token.wrong_syntax(message=str(err))
                else:
                    self.next_token = self.symbol_table['(float)'](self, value)
            elif '.' in literal:
                try:
                    value = Decimal(literal)
                except DecimalException as err:
                    self.next_token = self.symbol_table['(invalid)'](self, literal)
                    raise self.next_token.wrong_syntax(message=str(err))
                else:
                    self.next_token = self.symbol_table['(decimal)'](self, value)
            else:
                self.next_token = self.symbol_table['(integer)'](self, int(literal))

        elif name is not None:
            self.next_token = self.symbol_table['(name)'](self, name)
        elif unknown is not None:
            self.next_token = self.symbol_table['(unknown)'](self, unknown)
        else:
            msg = "unexpected matching %r: incompatible tokenizer"
            raise RuntimeError(msg % self.next_match.group())

        return self.token

    def advance_until(self, *stop_symbols: str) -> str:
        """
        Advances until one of the symbols is found or the end of source is reached,
        returning the raw source string placed before. Useful for raw parsing of
        comments and references enclosed between specific symbols.

        :param stop_symbols: The symbols that have to be found for stopping advance.
        :return: The source string chunk enclosed between the initial position \
        and the first stop symbol.
        """
        if not stop_symbols:
            raise self.next_token.wrong_type("at least a stop symbol required!")
        elif self.next_token.symbol == '(end)':
            raise self.next_token.wrong_syntax()

        self.token = self.next_token
        source_chunk: list[str] = []
        while True:
            try:
                self.next_match = next(self.tokens)
            except StopIteration:
                self.next_token = self.symbol_table['(end)'](self)
                break
            else:
                symbol = self.next_match.group(2)
                if symbol is not None:
                    symbol = symbol.strip()
                    if symbol not in stop_symbols:
                        source_chunk.append(symbol)
                    else:
                        try:
                            self.next_token = self.symbol_table[symbol](self)
                            break
                        except KeyError:
                            self.next_token = self.symbol_table['(unknown)'](self)
                            raise self.next_token.wrong_syntax()
                else:
                    source_chunk.append(self.next_match.group())
        return ''.join(source_chunk)

    def expression(self, rbp: int = 0) -> TK_co:
        """
        Pratt's function for parsing an expression. It calls token.nud() and then advances
        until the right binding power is less the left binding power of the next
        token, invoking the led() method on the following token.

        :param rbp: right binding power for the expression.
        :return: left token.
        """
        self.advance()
        left = self.token.nud()
        while rbp < self.next_token.lbp:
            self.advance()
            left = self.token.led(left)
        return cast(TK_co, left)

    @property
    def position(self) -> tuple[int, int]:
        """Property that returns the current line and column indexes."""
        return self.token.position

    def is_source_start(self) -> bool:
        """
        Returns `True` if the parser is positioned at the start
        of the source, ignoring the spaces.
        """
        return self.token.is_source_start()

    def is_line_start(self) -> bool:
        """
        Returns `True` if the parser is positioned at the start
        of a source line, ignoring the spaces.
        """
        return self.token.is_line_start()

    def is_spaced(self, before: bool = True, after: bool = True) -> bool:
        """
        Returns `True` if the source has an extra space (whitespace, tab or newline)
        immediately before or after the current position of the parser.

        :param before: if `True` considers also the extra spaces before \
        the current token symbol.
        :param after: if `True` considers also the extra spaces after \
        the current token symbol.
        """
        return self.token.is_spaced(before, after)

    @staticmethod
    def unescape(string_literal: str) -> str:
        return string_literal[1:-1].replace("\\'", "'").replace('\\"', '"')

    @classmethod
    def register(cls, symbol: str | type[TK_co], **kwargs: Any) -> type[TK_co]:
        """
        Register/update a token class in the symbol table.

        :param symbol: The identifier symbol for a new class or an existent token class.
        :param kwargs: Optional attributes/methods for the token class.
        :return: A token class.
        """
        token_class: type[TK_co]

        if isinstance(symbol, str):
            if ' ' in symbol:
                raise ValueError("%r: a symbol can't contain whitespaces" % symbol)

            lookup_name = kwargs.get('lookup_name', symbol)
            try:
                token_class = cls.symbol_table[lookup_name]
            except KeyError:
                # Register a new symbol and create a new custom class. The new token
                # class is registered globally in the module of the parser class.

                kwargs['symbol'] = symbol
                kwargs['lookup_name'] = lookup_name
                label = kwargs.get('label', 'symbol')
                if isinstance(label, tuple):
                    label = kwargs['label'] = MultiLabel(*label)

                if 'class_name' in kwargs:
                    token_class_name = kwargs.pop('class_name')
                else:
                    token_class_name = "_%s%s" % (
                        symbol_to_classname(symbol),
                        str(label).title().replace(' ', '')
                    )

                token_class_bases = kwargs.get('bases', (cls.token_base_class,))
                kwargs.update({
                    '__module__': cls.__module__,
                    '__qualname__': token_class_name,
                    '__return__': None
                })
                token_class = cast(
                    type[TK_co], ABCMeta(token_class_name, token_class_bases, kwargs)
                )
                cls.symbol_table[lookup_name] = token_class
                setattr(sys.modules[cls.__module__], token_class_name, token_class)

        elif not isinstance(symbol, type) or not issubclass(symbol, Token):
            raise TypeError("A string or a %r subclass requested, not %r." % (Token, symbol))
        else:
            token_class = symbol
            lookup_name = token_class.lookup_name

            if lookup_name not in cls.symbol_table:
                cls.symbol_table[lookup_name] = token_class
            elif cls.symbol_table[lookup_name] is not token_class:
                msg = "Token class {!r} collide on key {!r} with a different token class."
                raise ValueError(msg.format(token_class, lookup_name))

        for key, value in kwargs.items():
            if key == 'lbp' and value > token_class.lbp:
                token_class.lbp = value
            elif key == 'rbp' and value > token_class.rbp:
                token_class.rbp = value
            elif callable(value):
                setattr(token_class, key, value)

        return token_class

    @classmethod
    def unregister(cls, symbol: str) -> None:
        """Unregister a token class from the symbol table."""
        del cls.symbol_table[symbol.strip()]

    @classmethod
    def duplicate(cls, symbol: str, new_symbol: str, **kwargs: Any) -> type[TK_co]:
        """Duplicate a token class with a new symbol."""
        token_class = cls.symbol_table[symbol]
        new_token_class = cls.register(new_symbol, **kwargs)
        for key, value in token_class.__dict__.items():
            if key in kwargs or key in ('symbol', 'pattern') or key.startswith('_'):
                continue
            setattr(new_token_class, key, value)
        return new_token_class

    @classmethod
    def literal(cls, symbol: str, bp: int = 0) -> type[TK_co]:
        """Register a token for a symbol that represents a *literal*."""
        def nud(self: Token[TK_co]) -> Token[TK_co]:
            return self

        def evaluate(self: Token[TK_co], *_args: Any, **_kwargs: Any) -> Any:
            return self.value

        return cls.register(symbol, label='literal', lbp=bp, evaluate=evaluate, nud=nud)

    @classmethod
    def nullary(cls, symbol: str, bp: int = 0) -> type[TK_co]:
        """Register a token for a symbol that represents a *nullary* operator."""
        def nud(self: Token[TK_co]) -> Token[TK_co]:
            return self
        return cls.register(symbol, label='operator', lbp=bp, nud=nud)

    @classmethod
    def prefix(cls, symbol: str, bp: int = 0) -> type[TK_co]:
        """Register a token for a symbol that represents a *prefix* unary operator."""
        def nud(self: Token[TK_co]) -> Token[TK_co]:
            self[:] = self.parser.expression(rbp=bp),
            return self
        return cls.register(symbol, label='prefix operator', lbp=bp, rbp=bp, nud=nud)

    @classmethod
    def postfix(cls, symbol: str, bp: int = 0) -> type[TK_co]:
        """Register a token for a symbol that represents a *postfix* unary operator."""
        def led(self: Token[TK_co], left: Token[TK_co]) -> Token[TK_co]:
            self[:] = left,
            return self
        return cls.register(symbol, label='postfix operator', lbp=bp, rbp=bp, led=led)

    @classmethod
    def infix(cls, symbol: str, bp: int = 0) -> type[TK_co]:
        """Register a token for a symbol that represents an *infix* binary operator."""
        def led(self: Token[TK_co], left: Token[TK_co]) -> Token[TK_co]:
            self[:] = left, self.parser.expression(rbp=bp)
            return self
        return cls.register(symbol, label='operator', lbp=bp, rbp=bp, led=led)

    @classmethod
    def infixr(cls, symbol: str, bp: int = 0) -> type[TK_co]:
        """Register a token for a symbol that represents an *infixr* binary operator."""
        def led(self: Token[TK_co], left: Token[TK_co]) -> Token[TK_co]:
            self[:] = left, self.parser.expression(rbp=bp - 1)
            return self
        return cls.register(symbol, label='operator', lbp=bp, rbp=bp - 1, led=led)

    @classmethod
    def method(cls, symbol: str | type[TK_co], bp: int = 0, label: str = 'operator') \
            -> Callable[[Callable[..., RT]], Callable[..., RT]]:
        """
        Register a token for a symbol that represents a custom operator or redefine
        a method for an existing token.
        """
        token_class = cls.register(symbol, label=label, lbp=bp, rbp=bp)

        def bind(func: Callable[..., Any]) -> Callable[..., Any]:
            if '__' in func.__name__:
                method_name = func.__name__.partition('__')[0]
            elif func.__name__[0] != '_':
                method_name = func.__name__.partition('_')[0]
            else:
                method_name = f"_{func.__name__[1:].partition('_')[0]}"

            if not callable(getattr(token_class, method_name)):
                raise TypeError(f"{method_name!r} is not a method of {token_class}")
            setattr(token_class, method_name, func)
            return func
        return bind

    @classmethod
    def build(cls) -> None:
        """
        Builds the parser class. Checks if all declared symbols are defined
        and builds the regex tokenizer using the symbol related patterns.
        """
        # Register a minimal set of special tokens
        if '(start)' not in cls.symbol_table:
            cls.register('(start)')
        if '(end)' not in cls.symbol_table:
            cls.register('(end)')
        if '(invalid)' not in cls.symbol_table:
            cls.register('(invalid)')
        if '(unknown)' not in cls.symbol_table:
            cls.register('(unknown)')

        cls.tokenizer = cls.create_tokenizer(cls.symbol_table)

    @classmethod
    def create_tokenizer(cls, symbol_table: MutableMapping[str, type[TK_co]]) -> re.Pattern[str]:
        """
        Returns a regex based tokenizer built from a symbol table of token classes.
        The returned tokenizer skips extra spaces between symbols.

        A regular expression is created from the symbol table of the parser using a template.
        The symbols are inserted in the template putting the longer symbols first. Symbols and
        their patterns can't contain spaces.

        :param symbol_table: a dictionary containing the token classes of the formal language.
        """
        character_patterns = []
        string_patterns = []
        name_patterns = []
        custom_patterns = set()

        for symbol, token_class in symbol_table.items():
            if symbol in SPECIAL_SYMBOLS:
                continue
            elif token_class.pattern is not None:
                custom_patterns.add(token_class.pattern)
            elif cls.name_pattern.match(symbol) is not None:
                name_patterns.append(re.escape(symbol))
            elif len(symbol) == 1:
                character_patterns.append(re.escape(symbol))
            else:
                string_patterns.append(re.escape(symbol))

        symbols_patterns: list[str] = []
        if string_patterns:
            symbols_patterns.append('|'.join(sorted(string_patterns, key=lambda x: -len(x))))
        if character_patterns:
            symbols_patterns.append('[{}]'.format(''.join(character_patterns)))
        if name_patterns:
            symbols_patterns.append(r'\b(?:{})\b(?![\-\.])'.format(
                '|'.join(sorted(name_patterns, key=lambda x: -len(x)))
            ))
        if custom_patterns:
            symbols_patterns.append('|'.join(custom_patterns))

        tokenizer_pattern = r"({})|({})|({})|(\S)|\s+".format(
            cls.literals_pattern.pattern,
            '|'.join(symbols_patterns),
            cls.name_pattern.pattern
        )
        return re.compile(tokenizer_pattern)
