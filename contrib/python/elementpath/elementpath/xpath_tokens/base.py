#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import decimal
import math
import urllib.parse
from collections.abc import Iterator, Callable
from copy import copy
from decimal import Decimal
from itertools import product
from typing import Any, cast, ClassVar, SupportsFloat, TYPE_CHECKING, TypeVar

import elementpath.aliases as ta

from elementpath.exceptions import xpath_error, ElementPathError, ElementPathValueError, \
    ElementPathTypeError, MissingContextError
from elementpath.namespaces import XSD_ANY_TYPE, XSD_ANY_SIMPLE_TYPE, XSD_ANY_ATOMIC_TYPE
from elementpath.namespaces import XSD_NAMESPACE, XPATH_MATH_FUNCTIONS_NAMESPACE
from elementpath.datatypes import AnyAtomicType, AbstractDateTime, AnyURI, \
    DayTimeDuration, Date, DateTime, DecimalProxy, Duration, Integer, QName, \
    Timezone, UntypedAtomic, AbstractQName
from elementpath.tdop import Token, MultiLabel
from elementpath.helpers import ordinal, get_double
from elementpath.xpath_context import XPathContext, XPathSchemaContext
from elementpath.xpath_nodes import XPathNode, NamespaceNode, DocumentNode, ElementNode
from elementpath.sequences import xlist

if TYPE_CHECKING:
    from . import XPathFunction, XPathArray, XPathConstructor  # noqa: F401
    from . import NameToken, TokenRegistry  # noqa: F401

_XSD_SPECIAL_TYPES = frozenset((XSD_ANY_TYPE, XSD_ANY_SIMPLE_TYPE, XSD_ANY_ATOMIC_TYPE))
_CHILD_AXIS_TOKENS = frozenset((
    '*', 'node', 'child', 'text', '(name)', ':', '[', 'document-node',
    'element', 'comment', 'processing-instruction', 'schema-element'
))
_LEAF_ELEMENTS_TOKENS = frozenset((
    '(name)', '*', ':', '..', '.', '[', 'self', 'child', 'parent',
    'following-sibling', 'preceding-sibling', 'ancestor', 'ancestor-or-self',
    'descendant', 'descendant-or-self', 'following', 'preceding'
))

T = TypeVar('T', bound=ta.ItemType)


class XPathToken(Token[ta.XPathTokenType]):
    """Base class for XPath tokens."""
    registry: ClassVar['TokenRegistry']
    as_name: Callable[[], 'NameToken']

    parser: ta.XPathParserType
    value: ta.ValueType

    name: str = ''  # for storing the qualified name of a function
    namespace: str | None = None  # for namespace binding of names and wildcards
    occurrence: str = ''  # occurrence indicator, used by item types
    concatenated = False  # a flag for infix operators that can be concatenated

    def __str__(self) -> str:
        if self.symbol == '$':
            return '$%s variable reference' % (self[0].value if self._items else '')
        elif self.symbol == ',':
            return 'comma operator' if self.parser.version != '1.0' else 'comma symbol'
        elif self.symbol == '(':
            if not self or self[0].span[0] >= self.span[0]:
                return 'parenthesized expression'
            else:
                return 'function call expression'
        return super(XPathToken, self).__str__()

    @property
    def source(self) -> str:
        symbol = self.symbol
        if self.label == 'axis':
            # For XPath 2.0 'attribute' multirole token ('kind test', 'axis')
            return '%s::%s' % (symbol, self[0].source)
        elif symbol == '/' or symbol == '//':
            if not self:
                return symbol
            elif len(self) == 1:
                return f'{symbol}{self[0].source}'
            else:
                return f'{self[0].source}{symbol}{self[1].source}'
        elif symbol == '(':
            if not self:
                return '()'
            elif len(self) == 2:
                return f'{self[0].source}({self[1].source})'
            elif self[0].span[0] < self.span[0]:
                return f'{self[0].source}()'
            else:
                return f'({self[0].source})'
        elif symbol == '[':
            return '%s[%s]' % (self[0].source, self[1].source)
        elif symbol == ',':
            return '%s, %s' % (self[0].source, self[1].source)
        elif symbol == '$' or symbol == '@':
            return f'{symbol}{self[0].source}'
        elif symbol == '#':
            return '%s#%s' % (self[0].source, self[1].source)
        elif symbol == '{' or symbol == 'Q{':
            return '%s%s}%s' % (symbol, self[0].value, self[1].source)
        elif symbol == '=>':
            if isinstance(self[1], self.registry.function_token):
                return '%s => %s%s' % (self[0].source, self[1].symbol, self[2].source)
            return '%s => %s%s' % (self[0].source, self[1].source, self[2].source)
        elif symbol == 'if':
            return 'if (%s) then %s else %s' % (self[0].source, self[1].source, self[2].source)
        elif symbol == 'instance':
            return '%s instance of %s' % (
                self[0].source, ''.join(t.source for t in self[1:])
            )
        elif symbol in ('treat', 'cast', 'castable'):
            return '%s %s as %s' % (
                self[0].source, symbol, ''.join(t.source for t in self[1:])
            )
        elif symbol == 'for':
            return 'for %s return %s' % (
                ', '.join('%s in %s' % (self[k].source, self[k + 1].source)
                          for k in range(0, len(self) - 1, 2)),
                self[-1].source
            )
        elif symbol in ('every', 'some'):
            return '%s %s satisfies %s' % (
                symbol,
                ', '.join('%s in %s' % (self[k].source, self[k + 1].source)
                          for k in range(0, len(self) - 1, 2)),
                self[-1].source
            )
        elif symbol == 'let':
            return 'let %s return %s' % (
                ', '.join('%s := %s' % (self[k].source, self[k + 1].source)
                          for k in range(0, len(self) - 1, 2)),
                self[-1].source
            )
        elif symbol in ('-', '+') and len(self) == 1:
            return symbol + self[0].source
        return super(XPathToken, self).source

    @property
    def child_axis(self) -> bool:
        """Is `True` if the token apply child axis for default, `False` otherwise."""
        if self.symbol not in _CHILD_AXIS_TOKENS:
            return False
        elif self.symbol == '[':
            return self._items[0].child_axis
        elif self.symbol != ':':
            return True
        return not self._items[1].label.endswith('function')

    ###
    # Tokens tree analysis methods
    def iter_leaf_elements(self) -> Iterator[str]:
        """
        Iterates through the leaf elements of the token tree if there are any,
        returning QNames in prefixed format. A leaf element is an element
        positioned at last path step. Does not consider kind tests and wildcards.
        """
        if self.symbol in ('(name)', ':'):
            yield cast(str, self.value)
        elif self.symbol in ('//', '/'):
            if self._items[-1].symbol in _LEAF_ELEMENTS_TOKENS:
                yield from self._items[-1].iter_leaf_elements()

        elif self.symbol in ('[',):
            yield from self._items[0].iter_leaf_elements()
        else:
            for tk in self._items:
                yield from tk.iter_leaf_elements()

    ###
    # Evaluation and selectors methods
    def evaluate(self, context: ta.ContextType = None) -> ta.ValueType:
        """
        Evaluation method for XPath tokens.

        :param context: The XPath dynamic context.
        """
        return xlist(self.select(context))

    def select(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        """
        Select operator that generates XPath results, expanding sequences.

        :param context: The XPath dynamic context.
        """
        item = self.evaluate(context)
        if isinstance(item, list):
            yield from item
        else:
            yield item

    def select_flatten(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        """A select that flattens XPath results, including arrays."""
        for item in self.select(context):
            if isinstance(item, self.registry.array_token):
                yield from item.iter_flatten(context)
            else:
                yield item

    def select_with_focus(self, context: XPathContext) -> Iterator[ta.ItemType]:
        """Select item with an inner focus on dynamic context."""
        status = context.item, context.size, context.position, context.axis
        context.axis = None
        results = [x for x in self.select(context)]

        context.axis = None
        context.size = len(results)
        for context.position, context.item in enumerate(results, start=1):
            yield context.item

        context.item, context.size, context.position, context.axis = status

    def select_results(self, context: ta.ContextType) -> Iterator[ta.ResultType]:
        """
        Generates formatted XPath results.

        :param context: the XPath dynamic context.
        """
        if context is None:
            yield from self.select(context)
        else:
            self.parser.check_variables(context.variables)

            for result in self.select_flatten(context):
                if not isinstance(result, XPathNode):
                    yield result
                elif isinstance(result, NamespaceNode):
                    if self.parser.compatibility_mode:
                        yield result.prefix, result.uri
                    else:
                        yield result.uri
                elif isinstance(result, DocumentNode):
                    if result.is_extended:
                        # cannot represent with an ElementTree: yield the document node
                        yield result
                    elif result is context.root or result is not context.document:
                        yield result.value
                else:
                    yield result.value

    def get_results(self, context: ta.ContextType) -> \
            'list[ta.ResultType] | ta.AtomicType | XPathFunction':
        """
        Returns results formatted according to XPath specifications.

        :param context: the XPath dynamic context.
        :return: a list of nodes, atomic items or functions or a single atomic item or function.
        """
        results = list(self.select_results(context))
        if len(results) == 1:
            if hasattr(results[0], 'tag') or hasattr(results[0], 'getroot'):
                return results
            if isinstance(results[0], self.registry.function_token):
                return results[0]
            if self.symbol in ('.', '/', '//', '[', '(', '@', 'for',):
                return results
            if self.symbol in ('cast', 'castable', '-', '+', 'some', 'every'):
                return cast(AnyAtomicType, results[0])
            if self.label == 'kind test' or isinstance(self, self.registry.axis_token):
                return results
            if self.label == 'literal':
                return cast(AnyAtomicType, results[0])

            tk = self if self.symbol != ':' else self[1]
            if isinstance(tk, self.registry.function_token):
                rt = tk.sequence_types[-1]
                if rt.endswith(('*', '+')):
                    return results
                if rt.startswith('xs:'):
                    return cast(AnyAtomicType, results[0])
                return results

            if isinstance(results[0], AnyAtomicType):
                return results[0]
        return results

    ###
    # Helpers to get and validate arguments
    def get_argument(self, context: ta.ContextType,
                     index: int = 0,
                     required: bool = False,
                     default_to_context: bool = False,
                     default: ta.AtomicType | None = None,
                     cls: type[Any] | None = None,
                     promote: ta.ClassCheckType | None = None) -> Any:
        """
        Get the argument value of a function of constructor token. A zero length sequence is
        converted to a `None` value. If the function has no argument returns the context's
        item if the dynamic context is not `None`.

        :param context: the dynamic context.
        :param index: an index for select the argument to be got, the first for default.
        :param required: if set to `True` missing or empty sequence arguments are not allowed.
        :param default_to_context: if set to `True` then the item of the dynamic context is \
        returned when the argument is missing.
        :param default: the default value returned in case the argument is an empty sequence. \
        If not provided returns `None`.
        :param cls: if a type is provided performs a type checking on item.
        :param promote: a class or a tuple of classes that are promoted to `cls` class.
        """
        item: ta.ItemType | None
        try:
            token = self._items[index]
        except IndexError:
            if default_to_context:
                if context is None:
                    raise self.missing_context() from None
                item = context.item if context.item is not None else context.root
            elif isinstance(context, XPathSchemaContext):
                return default
            elif required:
                msg = "missing %s argument" % ordinal(index + 1)
                raise self.error('XPST0017', msg) from None
            else:
                return default
        else:
            if isinstance(token, XPathToken) and callable(token) and token.is_reference():
                return token  # It's a function reference

            item = None
            for k, result in enumerate(token.select(copy(context))):
                if k == 0:
                    item = result
                elif self.parser.compatibility_mode:
                    break
                elif isinstance(context, XPathSchemaContext):
                    # Multiple schema nodes are ignored but do not raise. The target
                    # of schema context selection is XSD type association and multiple
                    # node coherency is already checked at schema level.
                    break
                else:
                    msg = "a sequence of more than one item is not allowed as argument"
                    raise self.error('XPTY0004', msg)
            else:
                if item is None:
                    if not required or isinstance(context, XPathSchemaContext):
                        return default
                    ord_arg = ordinal(index + 1)
                    msg = "A not empty sequence required for {} argument"
                    raise self.error('XPTY0004', msg.format(ord_arg))

        if cls is not None:
            return self.validated_value(item, cls, promote, index)
        return item

    def get_argument_tokens(self) -> list['XPathToken']:
        """
        Builds and returns the argument tokens list, expanding the comma tokens.
        """
        tk = self
        tokens = []
        while True:
            if tk.symbol == ',':
                tokens.append(tk[1])
                tk = tk[0]
            else:
                tokens.append(tk)
                return tokens[::-1]

    def get_function(self, context: ta.ContextType, arity: int = 0) -> 'XPathFunction':
        if isinstance(self, self.registry.function_token):
            func = self
        elif self.symbol in (':', 'Q{') and isinstance(self[1], self.registry.function_token):
            func = self[1]
        elif self.symbol == '(name)':
            msg = f'unknown function: {self.value}#{arity}'
            raise self.error('XPST0017', msg)
        else:
            item = self.evaluate(context)
            if not isinstance(item, self.registry.function_token):
                msg = f'unknown function: {item}#{arity}'
                raise self.error('XPST0017', msg)
            func = item

        max_args = func.max_args
        if func.min_args > arity or max_args is not None and max_args < arity:
            msg = f'unknown function: {func.symbol}#{arity}'
            raise self.error('XPST0017', msg)

        return func

    def validated_value(self, item: Any, cls: type[Any],
                        promote: ta.ClassCheckType | None = None,
                        index: int | None = None) -> Any:
        """
        type promotion checking (see "function conversion rules" in XPath 2.0 language definition)
        """
        if isinstance(item, cls) or isinstance(item, XPathToken) and item.name == '(value)':
            return item
        elif promote and isinstance(item, promote):
            return cls(item)

        if self.parser.compatibility_mode:
            if issubclass(cls, str):
                return self.string_value(item)
            elif issubclass(cls, float) or issubclass(float, cls):
                return self.number_value(item)

        if issubclass(cls, XPathToken) or self.parser.version == '1.0':
            code = 'XPTY0004'
        else:
            value = self.data_value(item)

            if isinstance(value, cls):
                return value
            elif isinstance(value, AnyURI) and issubclass(cls, str):
                return cls(value)
            elif isinstance(value, UntypedAtomic):
                try:
                    return cls(value)
                except (TypeError, ValueError):
                    pass

            if value == []:
                code = 'FOTY0012'
            else:
                code = 'XPTY0004'

        if index is None:
            msg = f"item type is {type(item)!r} instead of {cls!r}"
        else:
            msg = f"{ordinal(index+1)} argument has type {type(item)!r} instead of {cls!r}"
        raise self.error(code, msg)

    ###
    # Atomization of sequences
    def atomization(self, context: ta.ContextType = None) -> Iterator[ta.AtomicType]:
        """
        Helper method for value atomization of a sequence.

        Ref: https://www.w3.org/TR/xpath31/#id-atomization

        :param context: the XPath dynamic context.
        """
        for item in self.select(copy(context)):
            yield from self.atomize_item(item)

    def atomize_item(self, item: ta.ValueType) -> Iterator[ta.AtomicType]:
        """
        Atomization of a sequence item. Yields typed values, as computed by
        fn:data().

        Ref: https://www.w3.org/TR/xpath31/#id-atomization
             https://www.w3.org/TR/xpath20/#dt-typed-value
        """
        match item:
            case None:
                return
            case XPathNode():
                if self.parser.version != '1.0':
                    value = None
                    for value in item.iter_typed_values:
                        yield value

                    if value is None:
                        msg = f"argument node {item!r} does not have a typed value"
                        raise self.error('FOTY0012', msg)
                else:
                    value = item.compat_string_value
                    yield value

            case list():
                for v in item:
                    yield from self.atomize_item(v)

            case XPathToken():
                if not callable(item):
                    msg = f"sequence item {item!r} is not appropriate for the context"
                    raise self.error('XPTY0004', msg)
                elif item.label != 'array':
                    raise self.error('FOTY0013', f"{item.label!r} has no typed value")

                for v in cast('XPathArray', item).iter_flatten():
                    if isinstance(v, AnyAtomicType):
                        yield v
                    else:
                        yield from self.atomize_item(v)

            case AnyAtomicType():
                yield cast(ta.AtomicType, item)
            case bytes():
                yield item.decode()
            case _:
                msg = f"sequence item {item!r} is not appropriate for the context"
                raise self.error('XPTY0004', msg)

    ###
    # Helpers for operators
    def get_atomized_operand(self, context: ta.ContextType = None) -> ta.AtomicType | None:
        """
        Get the atomized value for an XPath operator.

        :param context: the XPath dynamic context.
        :return: the atomized value of a single length sequence or `None` if the sequence is empty.
        """
        value = None
        first = True
        for value in self.atomization(context):
            if not first:
                msg = "atomized operand is a sequence of length greater than one"
                raise self.error('XPTY0004', msg)
            first = False
        else:
            if isinstance(value, UntypedAtomic):
                return str(value)
            else:
                return value

    def iter_comparison_data(self, context: ta.ContextType) -> Iterator[Any]:
        """
        Generates comparison data couples for the general comparison of sequences.
        Different sequences maybe generated with an XPath 2.0 parser, depending on
        compatibility mode setting.

        Ref: https://www.w3.org/TR/xpath20/#id-general-comparisons

        :param context: the XPath dynamic context.
        """
        left_values: Any
        right_values: Any
        msg = "cannot compare {!r} and {!r}"

        if self.parser.compatibility_mode:
            left_values = [x for x in self._items[0].atomization(context)]
            right_values = [x for x in self._items[1].atomization(context)]
            # Boolean comparison if one of the results is a single boolean value (1.)
            try:
                if isinstance(left_values[0], bool):
                    if len(left_values) == 1:
                        yield left_values[0], self.boolean_value(right_values)
                        return
                if isinstance(right_values[0], bool):
                    if len(right_values) == 1:
                        yield self.boolean_value(left_values), right_values[0]
                        return
            except IndexError:
                return

            # Converts to float for lesser-greater operators (3.)
            if self.symbol in ('<', '<=', '>', '>='):
                yield from product(map(float, left_values), map(float, right_values))
                return
            elif self.parser.version == '1.0':
                yield from product(left_values, right_values)
                return
        else:
            left_values = self._items[0].atomization(context)
            right_values = self._items[1].atomization(context)

        for op1, op2 in product(left_values, right_values):
            match op1:
                case str() | AnyURI():
                    if not isinstance(op2, (str, UntypedAtomic, AnyURI)):
                        raise TypeError(msg.format(type(op1), type(op2)))
                case bool():
                    if isinstance(op2, (str, Integer, AbstractQName, AnyURI)):
                        raise TypeError(msg.format(type(op1), type(op2)))
                case Integer():
                    if isinstance(op2, (str, AbstractQName, AnyURI, bool)):
                        raise TypeError(msg.format(type(op1), type(op2)))
                case float():
                    if isinstance(op2, decimal.Decimal):
                        yield op1, float(op2)
                        continue
                    elif isinstance(op2, (str, AbstractQName, AnyURI, bool)):
                        raise TypeError(msg.format(type(op1), type(op2)))
                case decimal.Decimal():
                    if isinstance(op2, float):
                        yield float(op1), op2
                        continue
                    elif isinstance(op2, (str, AbstractQName, AnyURI, bool)):
                        raise TypeError(msg.format(type(op1), type(op2)))
                case AbstractQName():
                    if not isinstance(op2, (AbstractQName, UntypedAtomic)):
                        raise TypeError(msg.format(type(op1), type(op2)))

            yield op1, op2

    def get_operands(self, context: ta.ContextType, cls: type[Any] | None = None) -> Any:
        """
        Returns the operands for a binary operator. Float arguments are converted
        to decimal if the other argument is a `Decimal` instance.

        :param context: the XPath dynamic context.
        :param cls: if a type is provided performs a type checking on item.
        :return: a couple of values representing the operands. If any operand \
        is not available returns a `(None, None)` couple.
        """
        op1 = self.get_argument(context, cls=cls)
        if op1 is None:
            return None, None
        elif isinstance(op1, ElementNode):
            op1 = self._items[0].data_value(op1)

        op2 = self.get_argument(context, index=1, cls=cls)
        if op2 is None:
            return None, None
        elif isinstance(op2, ElementNode):
            op2 = self._items[1].data_value(op2)

        if isinstance(op1, AbstractDateTime) and isinstance(op2, AbstractDateTime):
            if context is not None and context.timezone is not None:
                if op1.tzinfo is None:
                    op1.tzinfo = context.timezone
                if op2.tzinfo is None:
                    op2.tzinfo = context.timezone
        else:
            if isinstance(op1, UntypedAtomic):
                op1 = self.cast_to_double(op1.value)
                if isinstance(op2, Decimal):
                    return op1, float(op2)
            if isinstance(op2, UntypedAtomic):
                op2 = self.cast_to_double(op2.value)
                if isinstance(op1, Decimal):
                    return float(op1), op2

        if isinstance(op1, float):
            if isinstance(op2, Duration):
                return Decimal(op1), op2
            if isinstance(op2, Decimal):
                return op1, type(op1)(op2)
        if isinstance(op2, float):
            if isinstance(op1, Duration):
                return op1, Decimal(op2)
            if isinstance(op1, Decimal):
                return type(op2)(op1), op2

        return op1, op2

    def get_absolute_uri(self, uri: str, base_uri: str | None = None) -> str:
        """
        Obtains an absolute URI from the argument and the static context.

        :param uri: a string representing a URI.
        :param base_uri: an alternative base URI, otherwise the base_uri \
        of the static context is used.
        :returns: the argument if it's an absolute URI, otherwise returns the URI
        obtained by the join o the base_uri of the static context with the
        argument. Returns the argument if the base_uri is `None`.
        """
        if not base_uri:
            base_uri = self.parser.base_uri

        uri_parts: urllib.parse.ParseResult = urllib.parse.urlparse(uri)
        if uri_parts.scheme or uri_parts.netloc or base_uri is None:
            return uri

        base_uri_parts: urllib.parse.SplitResult = urllib.parse.urlsplit(base_uri)
        if base_uri_parts.fragment or not base_uri_parts.scheme and \
                not base_uri_parts.netloc and not base_uri_parts.path.startswith('/'):
            raise self.error('FORG0002', '{!r} is not suitable as base URI'.format(base_uri))

        if uri_parts.path.startswith('/') and base_uri_parts.path not in ('', '/'):
            return uri
        return urllib.parse.urljoin(base_uri, uri)

    def bind_namespace(self, namespace: str) -> None:
        """
        Bind a token with a namespace. The token has to be a name, a name wildcard,
        a function or a constructor, otherwise a syntax error is raised. Functions
        and constructors must be limited to their namespaces.
        """
        if self.symbol in ('(name)', '*') or isinstance(self, self.registry.proxy_token):
            pass
        elif namespace == self.parser.function_namespace:
            if self.label != 'function' and self.label != 'external function':
                msg = "a name, a wildcard or a function expected"
                raise self.wrong_syntax(msg, code='XPST0017')
            elif isinstance(self.label, MultiLabel):
                self.label = 'function'
        elif namespace == XSD_NAMESPACE:
            if self.label != 'constructor function':
                msg = "a name, a wildcard or a constructor function expected"
                raise self.wrong_syntax(msg, code='XPST0017')
            elif isinstance(self.label, MultiLabel):
                self.label = 'constructor function'
        elif namespace == XPATH_MATH_FUNCTIONS_NAMESPACE:
            if self.label != 'math function':
                msg = "a name, a wildcard or a math function expected"
                raise self.wrong_syntax(msg, code='XPST0017')
            elif isinstance(self.label, MultiLabel):
                self.label = 'math function'
        elif not isinstance(self, self.registry.function_token):
            msg = "a name, a wildcard or a function expected"
            raise self.wrong_syntax(msg, code='XPST0017')
        elif self.namespace and namespace != self.namespace:
            msg = "unmatched namespace"
            raise self.wrong_syntax(msg, code='XPST0017')

        self.namespace = namespace
        self.name = f'{{{namespace}}}{self.value}'

    def adjust_datetime(self, context: ta.ContextType, cls: type[AbstractDateTime]) \
            -> ta.OneOrEmpty[AbstractDateTime | DayTimeDuration]:
        """
        XSD datetime adjust function helper.

        :param context: the XPath dynamic context.
        :param cls: the XSD datetime subclass to use.
        :return: an empty list if there is only one argument that is the empty sequence \
        or the adjusted XSD datetime instance.
        """
        timezone: Any | None
        item: AbstractDateTime | None
        _item: AbstractDateTime | DayTimeDuration

        if len(self) == 1:
            item = self.get_argument(context, cls=cls)
            if item is None:
                return []
            timezone = getattr(context, 'timezone', None)
        else:
            item = self.get_argument(context, cls=cls)
            timezone = self.get_argument(context, 1, cls=DayTimeDuration)

            if timezone is not None:
                try:
                    timezone = Timezone.fromduration(timezone)
                except ValueError as err:
                    if isinstance(context, XPathSchemaContext):
                        timezone = Timezone.fromduration(DayTimeDuration(0))
                    else:
                        raise self.error('FODT0003', str(err)) from None
            if item is None:
                return []

        _item = copy(item)
        _tzinfo = _item.tzinfo
        try:
            if isinstance(_tzinfo, Timezone) and isinstance(timezone, Timezone):
                if isinstance(_item, DateTime):
                    _item += timezone.offset
                elif not isinstance(item, Date):
                    _item += timezone.offset - _tzinfo.offset
                elif timezone.offset < _tzinfo.offset:
                    _item -= timezone.offset - _tzinfo.offset
                    _item -= DayTimeDuration.fromstring('P1D')
        except OverflowError as err:
            if isinstance(context, XPathSchemaContext):
                return _item
            raise self.error('FODT0001', str(err)) from None

        if isinstance(_item, AbstractDateTime):
            _item.tzinfo = timezone
        return _item

    ###
    # Helpers for cast to XSD types
    def cast_to_qname(self, value: str) -> QName:
        """Cast a string compatible value to a QName object."""
        try:
            return QName.make(value, parser=self.parser)
        except ValueError:
            msg = 'invalid value {!r} for an xs:QName'.format(value)
            raise self.error('FORG0001', msg) from None
        except KeyError as err:
            msg = 'no namespace found for prefix {}'.format(err)
            raise self.error('FONS0004', msg) from None
        except TypeError:
            msg = 'the argument has an invalid type {!r}'.format(type(value))
            raise self.error('XPTY0004', msg) from None

    def cast_to_double(self, value: SupportsFloat | str) -> float:
        """Cast a value to xs:double."""
        try:
            return get_double(value, self.parser.xsd_version)
        except ValueError as err:
            raise self.error('FORG0001', str(err))  # str or UntypedAtomic

    def cast_to_primitive_type(self, value: Any, type_name: str) -> Any:
        """Cast a value to a primitive atomic type or a list of atomic values."""
        if value is None or not type_name.startswith('xs:') or type_name.count(':') != 1:
            return value

        type_name = type_name[3:].rstrip('+*?')
        token = cast('XPathConstructor', self.parser.symbol_table[type_name](self.parser))

        def cast_value(v: Any) -> Any:
            try:
                if isinstance(v, (UntypedAtomic, AnyURI)):
                    return token.cast(v)
                elif isinstance(v, (float, DecimalProxy)):
                    if type_name in ('double', 'float'):
                        return token.cast(v)
            except (ValueError, TypeError):
                return v
            else:
                return v

        if isinstance(value, list):
            return xlist([cast_value(x) for x in value])
        else:
            return cast_value(value)

    ###
    # XPath data accessors base functions
    def boolean_value(self, obj: Any) -> bool:
        """
        The effective boolean value, as computed by fn:boolean().
        """
        if isinstance(obj, list):
            if not obj:
                return False
            elif isinstance(obj[0], XPathNode):
                return True
            elif len(obj) > 1:
                message = "effective boolean value is not defined for a sequence " \
                          "of two or more items not starting with an XPath node."
                raise self.error('FORG0006', message)
            else:
                obj = obj[0]
        elif isinstance(obj, Iterator):
            k = 0
            items = obj
            obj = None
            for obj in items:
                if k:
                    message = "effective boolean value is not defined for a sequence " \
                              "of two or more items not starting with an XPath node."
                    raise self.error('FORG0006', message)
                elif isinstance(obj, XPathNode):
                    return True
                k += 1
            else:
                if not k:
                    return False

        if isinstance(obj, (int, str, UntypedAtomic, AnyURI)):  # Include bool
            return bool(obj)
        elif isinstance(obj, (float, Decimal)):
            return False if math.isnan(obj) else bool(obj)
        elif obj is None:
            return False
        elif isinstance(obj, XPathNode):
            return True
        else:
            message = "effective boolean value is not defined for {!r}.".format(type(obj))
            raise self.error('FORG0006', message)

    def data_value(self, obj: Any) -> ta.AtomicType | None:
        """
        Returns the typed value. Raises an error if the atomization of the value
        produces more than one typed value.
        """
        value = None
        first = True
        for value in self.atomize_item(obj):
            if not first:
                msg = "atomized value is a sequence of length greater than one"
                raise self.error('XPTY0004', msg)
            first = False
        else:
            return value

    def string_value(self, obj: Any) -> str:
        """
        The string value, as computed by fn:string().
        """
        if obj is None:
            return ''
        elif isinstance(obj, XPathNode):
            if self.parser.version == '1.0':
                return obj.compat_string_value
            return obj.string_value
        elif isinstance(obj, bool):
            return 'true' if obj else 'false'
        elif isinstance(obj, Decimal):
            value = format(obj, 'f')
            if '.' in value:
                return value.rstrip('0').rstrip('.')
            return value

        elif isinstance(obj, float):
            if math.isnan(obj):
                return 'NaN'
            elif math.isinf(obj):
                return str(obj).upper()

            value = str(obj)
            if '.' in value:
                value = value.rstrip('0').rstrip('.')
            if '+' in value:
                value = value.replace('+', '')
            if 'e' in value:
                return value.upper()
            return value

        elif isinstance(obj, self.registry.function_token):
            if self.symbol in ('concat', '||'):
                raise self.error('FOTY0013', f"an argument of {self} is a function")
            else:
                raise self.error('FOTY0014', f"{obj.label!r} has no string value")

        return str(obj)

    def number_value(self, obj: Any) -> float:
        """
        The numeric value, as computed by fn:number() on each item. Returns a float value.
        """
        try:
            if isinstance(obj, XPathNode):
                if self.parser.version == '1.0':
                    return get_double(obj.compat_string_value, self.parser.xsd_version)
                return get_double(obj.string_value, self.parser.xsd_version)
            else:
                return get_double(obj, self.parser.xsd_version)
        except (TypeError, ValueError):
            return math.nan

    ###
    # Error handling helpers and shortcuts
    def error(self, code: str | QName,
              message_or_error: str | Exception | None = None) -> ElementPathError:
        return xpath_error(code, message_or_error, self, self.parser.namespaces)

    def expected(self, *symbols: str,
                 message: str | None = None,
                 code: str = 'XPST0003') -> None:
        if symbols and self.symbol not in symbols:
            raise self.wrong_syntax(message, code)

    def unexpected(self, *symbols: str,
                   message: str | None = None,
                   code: str = 'XPST0003') -> None:
        if not symbols or self.symbol in symbols:
            raise self.wrong_syntax(message, code)

    def wrong_syntax(self, message: str | None = None,  # type: ignore[override]
                     code: str = 'XPST0003') -> ElementPathError:
        if self.label == 'function':
            code = 'XPST0017'

        if message:
            return self.error(code, message)

        error = super(XPathToken, self).wrong_syntax(message)
        return self.error(code, str(error))

    def wrong_value(self, message: str | None = None) -> ElementPathValueError:
        return cast(ElementPathValueError, self.error('FOCA0002', message))

    def wrong_type(self, message: str | None = None) -> ElementPathTypeError:
        return cast(ElementPathTypeError, self.error('FORG0006', message))

    def missing_context(self, message: str | None = None) -> MissingContextError:
        return cast(MissingContextError, self.error('XPDY0002', message))
