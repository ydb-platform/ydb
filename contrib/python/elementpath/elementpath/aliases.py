#
# Copyright (c), 2025-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""Type annotation aliases for elementpath."""
from collections.abc import MutableMapping, Iterator
from decimal import Decimal
from typing import Any, Optional, NoReturn, TYPE_CHECKING, TypeVar, Union

if TYPE_CHECKING:
    from .protocols import ElementProtocol, DocumentProtocol  # noqa: F401
    from .protocols import XsdElementProtocol, XsdAttributeProtocol  # noqa: F401
    from .protocols import DocumentType, ElementType, SchemaElemType  # noqa: F401
    from .datatypes import AnyAtomicType, AbstractDateTime, QName, AnyURI  # noqa: F401
    from .datatypes import Date, DateTime, Time, GregorianYear, GregorianMonth  # noqa: F401
    from .datatypes import GregorianDay, GregorianYearMonth, GregorianMonthDay  # noqa: F401
    from .datatypes import DateTimeStamp, Duration, UntypedAtomic  # noqa: F401
    from .datatypes import DayTimeDuration, YearMonthDuration  # noqa: F401
    from .schema_proxy import AbstractSchemaProxy  # noqa: F401
    from .xpath_nodes import XPathNode, ElementNode, AttributeNode  # noqa: F401
    from .xpath_nodes import TextNode, DocumentNode, NamespaceNode  # noqa: F401
    from .xpath_nodes import CommentNode, ProcessingInstructionNode  # noqa: F401
    from .xpath_tokens import XPathToken, XPathAxis, XPathFunction  # noqa: F401
    from .xpath_tokens import XPathConstructor, XPathMap, XPathArray  # noqa: F401
    from .xpath_context import XPathContext, XPathSchemaContext  # noqa: F401
    from .xpath1 import XPath1Parser  # noqa: F401
    from .xpath2 import XPath2Parser  # noqa: F401
    from .xpath30 import XPath30Parser  # noqa: F401
    from .xpath31 import XPath31Parser  # noqa: F401

###
# Namespace maps
NamespacesType = MutableMapping[str, str]
NsmapType = MutableMapping[Optional[str], str]  # compatible with the nsmap of lxml Element
AnyNsmapType = Union[NamespacesType, NsmapType, None]  # for composition and function arguments

###
# Datatypes
NumericType = Union[int, float, Decimal]
DateTimeType = Union[
    'DateTime', 'Date', 'Time', 'GregorianYear', 'GregorianMonth',
    'GregorianDay', 'GregorianYearMonth', 'GregorianMonthDay'
]
DurationType = Union['Duration', 'YearMonthDuration', 'DayTimeDuration']
ArithmeticType = Union[NumericType, DateTimeType, 'DurationType', 'UntypedAtomic']
AtomicType = Union['AnyAtomicType', str, bool, int, float, Decimal]

###
# XPath nodes
TypedNodeType = Union['AttributeNode', 'ElementNode']
TaggedNodeType = Union['ElementNode', 'CommentNode', 'ProcessingInstructionNode']
ElementMapType = dict[object, TaggedNodeType]
FindAttrType = Optional['XsdAttributeProtocol']
FindElemType = Optional['XsdElementProtocol']
RootNodeType = Union['DocumentNode', 'ElementNode']

ParentNodeType = Union['DocumentNode', 'ElementNode']
ChildNodeType = Union['TextNode', TaggedNodeType]

# All possible variants for node as item
XPathNodeType = Union['XPathNode', 'DocumentNode', 'NamespaceNode', 'AttributeNode',
                      'TextNode', 'ElementNode', 'CommentNode', 'ProcessingInstructionNode']

###
# Sequence and item/value types (specifics and generics)
ItemType = Union['XPathNode', AtomicType, 'XPathFunction']

_T = TypeVar('_T', bound=ItemType)

OneOrEmpty = Union[_T, list[_T], list[NoReturn]]
OneOrMore = Union[_T, list[_T]]
AnyOrEmpty = Union[_T, list[_T], list[NoReturn]]

SequenceArgType = Union[
    Iterator[_T], list[_T], list[NoReturn], tuple[_T, ...], tuple[_T], tuple[()]
]

OneAtomicOrEmpty = OneOrEmpty[AtomicType]
OneNumericOrEmpty = OneOrEmpty[NumericType]
OneArithmeticOrEmpty = OneOrEmpty[ArithmeticType]
OneItemOrEmpty = OneOrEmpty[ItemType]

OneOrMoreAtomic = OneOrMore[AtomicType]
OneOrMoreNumeric = OneOrMore[NumericType]
OneOrMoreArithmetic = OneOrMore[ArithmeticType]
OneOrMoreItems = OneOrMore[ItemType]

AnyAtomicOrEmpty = AnyOrEmpty[AtomicType]
AnyNumericOrEmpty = AnyOrEmpty[NumericType]
AnyArithmeticOrEmpty = AnyOrEmpty[ArithmeticType]
AnyItemsOrEmpty = AnyOrEmpty[ItemType]

ValueType = Union[ItemType, list[ItemType], list[NoReturn], list['XPathNode'],
                  list[Union['AttributeNode', 'ElementNode']], list[AtomicType]]
ListItemType = Union[ItemType, list[ItemType], list[NoReturn]]
ResultType = Union[
    AtomicType, 'ElementProtocol', 'XsdAttributeProtocol', tuple[Optional[str], str],
    'DocumentProtocol', 'DocumentNode', 'XPathFunction', object
]
MapDictType = dict[Optional[AtomicType], ValueType]
SequenceTypesType = Union[str, list[str], tuple[str, ...]]

# Parsers and tokens
XPathParserType = Union['XPath1Parser', 'XPath2Parser', 'XPath30Parser', 'XPath31Parser']
XPathTokenType = Union['XPathToken', 'XPathAxis', 'XPathFunction', 'XPathConstructor']
XPath2ParserType = Union['XPath2Parser', 'XPath30Parser', 'XPath31Parser']
ParserClassType = Union[
    type['XPath1Parser'], type['XPath2Parser'], type['XPath30Parser'], type['XPath31Parser']
]
NargsType = Optional[Union[int, tuple[int, Optional[int]]]]
ClassCheckType = Union[type[Any], tuple[type[Any], ...]]

# Context arguments
ContextType = Union['XPathContext', 'XPathSchemaContext', None]
RootArgType = Union['DocumentType', 'ElementType', 'SchemaElemType', RootNodeType]
ItemArgType = Union[ItemType, 'ElementProtocol', 'DocumentProtocol']
SchemaProxyType = Union['AbstractSchemaProxy']

_S = TypeVar('_S', bound=ItemArgType)

InputType = Union[None, _S, list[_S], list[NoReturn], tuple[_S, ...], tuple[_S], tuple[()]]
VariableValueType = InputType[ItemArgType]
FunctionArgType = Union[VariableValueType, ValueType]
NodeArgType = Union['XPathNode', 'ElementProtocol', 'DocumentProtocol']
CollectionArgType = InputType[NodeArgType]
