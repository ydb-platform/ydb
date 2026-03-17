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
XPath 1.0 implementation - part 1 (parser class and symbols)
"""
import re
from abc import ABCMeta
from collections.abc import Callable, MutableMapping, Sequence
from typing import cast, Any, ClassVar, Optional, Union

import elementpath.aliases as ta

from elementpath.exceptions import xpath_error, UnsupportedFeatureError, \
    ElementPathValueError, ElementPathNameError, ElementPathKeyError, MissingContextError
from elementpath.namespaces import XML_NAMESPACE, XSD_NAMESPACE, XPATH_FUNCTIONS_NAMESPACE
from elementpath.helpers import upper_camel_case
from elementpath.collations import UNICODE_CODEPOINT_COLLATION
from elementpath.datatypes import QName
from elementpath.tdop import Parser
from elementpath.sequence_types import get_function_signatures, match_sequence_type
from elementpath.schema_proxy import AbstractSchemaProxy
from elementpath.xpath_tokens import XPathToken, XPathAxis, XPathFunction, ProxyToken, \
    NameToken, PrefixedNameToken, BracedNameToken


class XPath1Parser(Parser[ta.XPathTokenType]):
    """
    XPath 1.0 expression parser class. Provide a *namespaces* dictionary argument for
    mapping namespace prefixes to URI inside expressions. If *strict* is set to `False`
    the parser enables also the parsing of QNames, like the ElementPath library.

    :param namespaces: a dictionary with mapping from namespace prefixes into URIs.
    :param strict: a strict mode is `False` the parser enables parsing of QNames \
    in extended format, like the Python's ElementPath library. Default is `True`.
    """
    version = '1.0'
    """The XPath version string."""

    token_base_class = XPathToken  # type: ignore[assignment, unused-ignore]
    literals_pattern = re.compile(
        r"""'(?:[^']|'')*'|"(?:[^"]|"")*"|(?:\d+|\.\d+)(?:\.\d*)?(?:[Ee][+-]?\d+)?"""
    )
    name_pattern = re.compile(r'[^\d\W][\w.\-\xb7\u0300-\u036F\u203F\u2040]*')

    RESERVED_FUNCTION_NAMES = {
        'comment', 'element', 'node', 'processing-instruction', 'text'
    }

    DEFAULT_NAMESPACES: ClassVar[dict[str, str]] = {'xml': XML_NAMESPACE}
    """Namespaces known statically by default."""

    # Labels and symbols admitted after a path step
    PATH_STEP_LABELS: ClassVar[tuple[str, ...]] = ('axis', 'kind test')
    PATH_STEP_SYMBOLS: ClassVar[set[str]] = {
        '(integer)', '(string)', '(float)', '(decimal)', '(name)', '*', '@', '..', '.', '{'
    }

    # Class attributes for compatibility with XPath 2.0+
    schema: Optional[AbstractSchemaProxy] = None
    variable_types: Optional[dict[str, str]] = None
    document_types: Optional[dict[str, str]] = None
    collection_types: Optional[ta.NamespacesType] = None
    default_collection_type: str = 'node()*'
    base_uri: Optional[str] = None
    function_namespace = XPATH_FUNCTIONS_NAMESPACE
    function_signatures: dict[tuple[QName, int], str] = {}
    decimal_formats: dict[Optional[str], Any] = {}
    parse_arguments: bool = True
    defuse_xml: bool = True

    compatibility_mode: bool = True
    """XPath 1.0 compatibility mode."""

    default_namespace: Optional[str] = None
    """
    The default namespace. For XPath 1.0 this value is always `None` because the default
    namespace is ignored (see https://www.w3.org/TR/1999/REC-xpath-19991116/#node-tests).
    """

    default_collation = UNICODE_CODEPOINT_COLLATION

    @staticmethod
    def tracer(trace_data: str) -> None:
        """Trace data collector"""

    def __init__(self, namespaces: Optional[ta.NamespacesType] = None,
                 strict: bool = True) -> None:
        super(XPath1Parser, self).__init__()
        self.namespaces: dict[str, str] = self.DEFAULT_NAMESPACES.copy()
        if namespaces is not None:
            self.namespaces.update(namespaces)
        self.strict: bool = strict

    def __str__(self) -> str:
        args = []
        if self.namespaces != self.DEFAULT_NAMESPACES:
            args.append(str(self.other_namespaces))
        if not self.strict:
            args.append('strict=False')
        return f"{self.__class__.__name__}({', '.join(args)})"

    @property
    def other_namespaces(self) -> dict[str, str]:
        """The subset of namespaces not known by default."""
        return {k: v for k, v in self.namespaces.items()
                if k not in self.DEFAULT_NAMESPACES or self.DEFAULT_NAMESPACES[k] != v}

    @property
    def xsd_version(self) -> str:
        return '1.0'  # Use XSD 1.0 datatypes for default

    def is_schema_bound(self) -> bool:
        return False

    def xsd_qname(self, local_name: str) -> str:
        """Returns a prefixed QName string for XSD namespace."""
        if self.namespaces.get('xs') == XSD_NAMESPACE:
            return 'xs:%s' % local_name

        for pfx, uri in self.namespaces.items():
            if uri == XSD_NAMESPACE:
                return '%s:%s' % (pfx, local_name) if pfx else local_name

        raise xpath_error('XPST0081', 'Missing XSD namespace registration')

    @classmethod
    def create_restricted_parser(cls, name: str, symbols: Sequence[str]) \
            -> type['XPath1Parser']:
        """Get a parser subclass with a restricted set of symbols.s"""
        symbol_table = {
            k: v for k, v in cls.symbol_table.items() if k in symbols
        }
        return cast(type['XPath1Parser'], ABCMeta(
            f"{name}{cls.__name__}", (cls,), {'symbol_table': symbol_table}
        ))

    @staticmethod
    def unescape(string_literal: str) -> str:
        if string_literal.startswith("'"):
            return string_literal[1:-1].replace("''", "'")
        else:
            return string_literal[1:-1].replace('""', '"')

    @classmethod
    def proxy(cls, symbol: str, label: str = 'proxy', bp: int = 90) -> type[ProxyToken]:
        """Register a proxy token class for a symbol."""
        if symbol in cls.symbol_table and not issubclass(cls.symbol_table[symbol], ProxyToken):
            # Move the token class before register the proxy token
            token_cls = cls.symbol_table.pop(symbol)
            cls.symbol_table[f'{{{token_cls.namespace}}}{symbol}'] = token_cls

        token_class_name = "_%s%sProxy" % (
            upper_camel_case(symbol), str(label).title().replace(' ', '')
        )
        token_class = cls.register(
            symbol,
            label='function',
            class_name=token_class_name,
            bases=(ProxyToken,),
            lbp=bp,
            rbp=bp
        )
        assert issubclass(token_class, ProxyToken)
        return token_class

    @classmethod
    def axis(cls, symbol: str, reverse_axis: bool = False, bp: int = 80) -> type[XPathAxis]:
        """Register a token class for a symbol that represents an XPath *axis*."""
        token_class = cls.register(symbol, bases=(XPathAxis,),
                                   reverse_axis=reverse_axis, lbp=bp, rbp=bp)
        assert issubclass(token_class, XPathAxis)
        return token_class

    @classmethod
    def function(cls, symbol: str,
                 prefix: Optional[str] = None,
                 label: str = 'function',
                 nargs: ta.NargsType = None,
                 sequence_types: tuple[str, ...] = (),
                 bp: int = 90) -> type[XPathFunction]:
        """
        Registers a token class for a symbol that represents an XPath function.
        """
        kwargs: dict[str, Any] = {
            'bases': (XPathFunction,),
            'label': label,
            'nargs': nargs,
            'lbp': bp,
            'rbp': bp,
        }
        if 'function' not in label:
            # kind test or sequence type
            return cast(type[XPathFunction], cls.register(symbol, **kwargs))
        elif symbol in cls.RESERVED_FUNCTION_NAMES:
            raise ElementPathValueError(f'{symbol!r} is a reserved function name')

        if prefix:
            namespace = cls.DEFAULT_NAMESPACES[prefix]
            qname = QName(namespace, '%s:%s' % (prefix, symbol))
            kwargs['lookup_name'] = qname.expanded_name
            kwargs['class_name'] = '_%s%s%s' % (
                prefix.capitalize(),
                symbol.capitalize(),
                str(label).title().replace(' ', '')
            )
            kwargs['namespace'] = namespace
            cls.proxy(symbol, label='function', bp=bp)
        else:
            qname = QName(XPATH_FUNCTIONS_NAMESPACE, 'fn:%s' % symbol)
            kwargs['namespace'] = XPATH_FUNCTIONS_NAMESPACE

        if sequence_types:
            # Register function signature(s)
            cls.function_signatures.update(
                get_function_signatures(qname, nargs, sequence_types)
            )
            kwargs['sequence_types'] = sequence_types

        return cast(type[XPathFunction], cls.register(symbol, **kwargs))

    def parse(self, source: str) -> XPathToken:
        if self.tokenizer is None:
            self.tokenizer = self.create_tokenizer(self.symbol_table)

        root_token = super().parse(source)
        if root_token.label in ('sequence type', 'function test'):
            raise root_token.error('XPST0003', "not allowed in XPath expression")

        try:
            root_token.evaluate()  # Static context evaluation
        except MissingContextError:
            pass

        if self.schema is not None:
            # Static evaluation using a schema context
            context = self.schema.get_context()
            for _ in root_token.select(context):
                pass

        return root_token

    def expected_next(self, *symbols: str, message: Optional[str] = None) -> None:
        """
        Checks the next token with a list of symbols. Replaces the next token with
        a '(name)' token if the check fails and the next token can be a name,
        otherwise raises a syntax error.

        :param symbols: a sequence of symbols.
        :param message: optional error message.
        """
        if self.next_token.symbol in symbols:
            return
        elif '(name)' in symbols and \
                not isinstance(self.next_token, (XPathFunction, XPathAxis)) and \
                self.name_pattern.match(self.next_token.symbol) is not None:
            # Disambiguation replacing the next token with a '(name)' token
            cls = self.symbol_table['(name)']
            assert not issubclass(cls, XPathFunction)
            value = self.next_token.symbol
            self.next_token = cls(self, value)
        else:
            raise self.next_token.wrong_syntax(message)

    def parse_occurrence(self, token: XPathToken) -> None:
        """Parse the occurrence for the current token."""
        if self.next_token.symbol in ('*', '+', '?'):
            assert self.token is token
            token.occurrence = self.next_token.symbol
            self.advance()
            self.next_token.unexpected('*', '+', '?')

    def parse_sequence_type(self) -> XPathToken:
        if self.next_token.label in ('kind test', 'sequence type', 'function test'):
            token = self.expression(rbp=85)
        else:
            if self.next_token.symbol == 'Q{':
                token = self.advance().nud()
            elif self.next_token.symbol != '(name)':
                raise self.next_token.wrong_syntax()
            else:
                self.advance()
                if self.next_token.symbol == ':':
                    left = self.token
                    self.advance()
                    token = self.token.led(left)
                else:
                    token = self.token

                if self.next_token.symbol in ('::', '('):
                    raise self.next_token.wrong_syntax()

        next_symbol = self.next_token.symbol
        if token.symbol != 'empty-sequence' and next_symbol in ('?', '*', '+'):
            token.occurrence = next_symbol
            self.advance()
        return token

    def check_variables(self, values: MutableMapping[str, Any]) -> None:
        """Checks the sequence types of the XPath dynamic context's variables."""
        for varname, value in values.items():
            if not match_sequence_type(
                value, 'item()*' if isinstance(value, list) else 'item()', self
            ):
                message = "Unmatched sequence type for variable {!r}".format(varname)
                raise xpath_error('XPDY0050', message)

    def get_function(self, name: str, arity: Optional[int],
                     context: ta.ContextType = None) -> XPathFunction:
        """
        Returns an XPathFunction object suitable for stand-alone usage.

        :param name: the name of the function.
        :param arity: the arity of the function object, must be compatible \
        with the signature of the XPath function.
        :param context: an optional context to bound to the function.
        """
        if ':' not in name:
            qname = QName(XPATH_FUNCTIONS_NAMESPACE, f'fn:{name}')
        elif name.startswith('fn:'):
            qname = QName(XPATH_FUNCTIONS_NAMESPACE, name)
            name = name[3:]
        else:
            prefix, name = name.split(':')
            try:
                namespace = self.namespaces[prefix]
            except KeyError:
                raise ElementPathKeyError(f"Unknown namespace {prefix!r}") from None
            else:
                qname = QName(namespace, f'{prefix}:{name}')

        if qname.expanded_name in self.symbol_table:
            token_class = self.symbol_table[qname.expanded_name]
        elif name in self.symbol_table:
            token_class = self.symbol_table[name]
        else:
            raise ElementPathNameError(f'unknown function {name!r}')

        if not issubclass(token_class, XPathFunction):
            raise ElementPathNameError(f'{name!r} is not an XPath function')

        if token_class.namespace != qname.namespace:
            raise ElementPathNameError(f'namespace mismatch: {token_class.namespace}')

        try:
            func = token_class(self, nargs=arity)
        except TypeError:
            msg = f"unknown function {qname.qname}#{arity}"
            raise xpath_error('XPST0017', msg) from None
        else:
            if context is not None:
                func.context = context
            return func

    ###
    # Unsupported methods in XPath 1.0
    @classmethod
    def constructor(cls, symbol: str, bp: int = 90, nargs: ta.NargsType = 1,
                    sequence_types: Union[tuple[()], tuple[str, ...], list[str]] = (),
                    label: Union[str, tuple[str, ...]] = 'constructor function') \
            -> Callable[[Callable[..., Any]], Callable[..., Any]]:
        """
        Statically creates a constructor token class, that is registered in the globals
        of the module where the method is called.
        """
        raise UnsupportedFeatureError("Static definition of schema constructors token "
                                      "classes requires an XPath 2.0+ parser")

    def schema_constructor(self, atomic_type_name: str, bp: int = 90) \
            -> type[XPathFunction]:
        """Dynamically registers a token class for a schema atomic type constructor function."""
        raise UnsupportedFeatureError("Dynamic definition of schema constructors token "
                                      "classes requires an XPath 2.0+ parser")

    def external_function(self,
                          callback: Callable[..., Any],
                          name: Optional[str] = None,
                          prefix: Optional[str] = None,
                          sequence_types: tuple[str, ...] = (),
                          bp: int = 90) -> type[XPathFunction]:
        """Registers a token class for an external function."""
        raise UnsupportedFeatureError(
            "Registration of external functions requires an XPath 2.0+ parser"
        )


###
# Special symbols
XPath1Parser.register('(start)')
XPath1Parser.register('(end)')
XPath1Parser.register('(invalid)')
XPath1Parser.register('(unknown)')
XPath1Parser.literal('(string)')
XPath1Parser.literal('(float)')
XPath1Parser.literal('(decimal)')
XPath1Parser.literal('(integer)')

###
# Simple symbols
XPath1Parser.register(',')
XPath1Parser.register(')', bp=100)
XPath1Parser.register(']')
XPath1Parser.register('::')
XPath1Parser.register('}')

###
# Name related tokens
XPath1Parser.symbol_table['(name)'] = NameToken
XPath1Parser.symbol_table[':'] = PrefixedNameToken
XPath1Parser.symbol_table['{'] = BracedNameToken

# XPath 1.0 definitions continue into module _xpath1_operators
