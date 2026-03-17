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
XPath 2.0 implementation - part 4 (XSD constructors)
"""
import decimal
from typing import cast, NoReturn

import elementpath.aliases as ta

from elementpath.exceptions import ElementPathError, ElementPathSyntaxError
from elementpath.namespaces import XSD_NAMESPACE
from elementpath.datatypes import AbstractDateTime, Duration, Date, DateTime, \
    DateTimeStamp, Time, UntypedAtomic, QName, HexBinary, Base64Binary, \
    BooleanProxy, AnyURI, Notation, NMToken, Idref, Entity, DateTime10, ErrorProxy
from elementpath.xpath_context import XPathSchemaContext
from elementpath.xpath_tokens import XPathConstructor
from elementpath.sequences import xlist
from ._xpath2_functions import XPath2Parser

register = XPath2Parser.register
unregister = XPath2Parser.unregister
method = XPath2Parser.method
constructor = XPath2Parser.constructor


@constructor('normalizedString')
@constructor('token')
@constructor('language')
@constructor('NMTOKEN')
@constructor('Name')
@constructor('NCName')
@constructor('ID')
@constructor('IDREF')
@constructor('ENTITY')
@constructor('anyURI')
def cast__string_types(self: XPathConstructor, value: ta.AtomicType) -> str | AnyURI:
    try:
        result = cast(str | AnyURI, self.type_class.make(value))
    except ValueError as err:
        raise self.error('FORG0001', err)
    else:
        assert isinstance(result, (str, AnyURI))
        return result


@constructor('decimal')
@constructor('double')
@constructor('float')
def cast__numeric_types(self: XPathConstructor, value: ta.AtomicType) -> ta.NumericType:
    try:
        result = self.type_class.make(value, parser=self.parser)
    except ValueError as err:
        if isinstance(value, (str, UntypedAtomic)):
            raise self.error('FORG0001', err)
        raise self.error('FOCA0002', err)
    except ArithmeticError as err:
        raise self.error('FOCA0002', err) from None
    else:
        assert isinstance(result, (int, float, decimal.Decimal))
        return result


@constructor('integer')
@constructor('nonNegativeInteger')
@constructor('positiveInteger')
@constructor('nonPositiveInteger')
@constructor('negativeInteger')
@constructor('long')
@constructor('int')
@constructor('short')
@constructor('byte')
@constructor('unsignedLong')
@constructor('unsignedInt')
@constructor('unsignedShort')
@constructor('unsignedByte')
def cast__integer_types(self: XPathConstructor, value: ta.AtomicType) -> int:
    try:
        return cast(int, self.type_class.make(value))
    except ValueError:
        msg = 'could not convert {!r} to xs:{}'.format(value, self.symbol)
        if isinstance(value, (str, bytes, int, UntypedAtomic)):
            raise self.error('FORG0001', msg) from None
        raise self.error('FOCA0002', msg) from None
    except ArithmeticError as err:
        raise self.error('FOCA0002', err) from None


@constructor('date')
@constructor('gDay')
@constructor('gMonth')
@constructor('gMonthDay')
@constructor('gYear')
@constructor('gYearMonth')
@constructor('time')
@constructor('dateTimeStamp')
def cast__other_datetime_types(self: XPathConstructor, value: ta.AtomicType) -> ta.DateTimeType:
    try:
        return cast(ta.DateTimeType, self.type_class).make(value, parser=self.parser)
    except OverflowError as err:
        raise self.error('FODT0001', err) from None
    except ValueError as err:
        raise self.error('FORG0001', err)


@method('date')
@method('gDay')
@method('gMonth')
@method('gMonthDay')
@method('gYear')
@method('gYearMonth')
@method('time')
def evaluate__other_datetime_types(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[AbstractDateTime]:
    if self.context is not None:
        context = self.context

    arg = self.data_value(self.get_argument(context))
    if arg is None:
        return []

    try:
        return cast(AbstractDateTime, self.cast(arg))
    except (TypeError, OverflowError) as err:
        if isinstance(context, XPathSchemaContext):
            return []
        elif isinstance(err, TypeError):
            raise self.error('FORG0006', err) from None
        else:
            raise self.error('FODT0001', err) from None


@method('dateTimeStamp')
def evaluate__datetime_stamp_type(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[DateTimeStamp]:
    if self.context is not None:
        context = self.context

    arg = self.data_value(self.get_argument(context))
    if arg is None:
        return []

    if isinstance(arg, UntypedAtomic):
        result = self.cast(arg.value)
    elif isinstance(arg, Date):
        result = self.cast(arg)
    else:
        result = self.cast(str(arg))
    assert isinstance(result, DateTimeStamp)
    return result


@method('dateTimeStamp')
def nud__datetime_stamp_type(self: XPathConstructor) -> XPathConstructor:
    if self.parser.xsd_version == '1.0':
        raise self.wrong_syntax("xs:dateTimeStamp is not recognized unless XSD 1.1 is enabled")
    if not self.parser.parse_arguments:
        return self

    try:
        self.parser.advance('(')
        self[0:] = self.parser.expression(5),
        if self.parser.next_token.symbol == ',':
            msg = 'Too many arguments: expected at most 1 argument'
            raise self.error('XPST0017', msg)
        self.parser.advance(')')
    except SyntaxError as err:
        raise self.error('XPST0017', str(err)) from None
    return self


@constructor('duration')
@constructor('yearMonthDuration')
@constructor('dayTimeDuration')
def cast__duration_types(self: XPathConstructor, value: ta.AtomicType) -> Duration:
    try:
        return cast(Duration, self.type_class).make(value)
    except OverflowError as err:
        raise self.error('FODT0002', err) from None
    except ValueError as err:
        raise self.error('FORG0001', err)


###
# Constructors for binary XSD types
@constructor('base64Binary')
@constructor('hexBinary')
def cast__binary_types(self: XPathConstructor, value: ta.AtomicType) -> Base64Binary | HexBinary:
    try:
        result = self.type_class.make(value, parser=self.parser)
    except ValueError as err:
        raise self.error('FORG0001', err) from None
    except TypeError as err:
        raise self.error('XPTY0004', err) from None
    else:
        assert isinstance(result, (Base64Binary, HexBinary))
        return result


@method('base64Binary')
@method('hexBinary')
def evaluate__binary_types(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[HexBinary | Base64Binary]:
    arg = self.data_value(self.get_argument(self.context or context))
    if arg is None:
        return []

    try:
        return cast(HexBinary | Base64Binary, self.cast(arg))
    except ElementPathError as err:
        if isinstance(context, XPathSchemaContext):
            return []
        err.token = self
        raise


@constructor('NOTATION')
def cast__notation_type(self: XPathConstructor, value: ta.AtomicType) -> Notation:
    raise NotImplementedError("No value is castable to xs:NOTATION")


@method('NOTATION')
def nud__notation_type(self: XPathConstructor) -> None:
    if not self.parser.parse_arguments:
        return

    self.parser.advance('(')
    if self.parser.next_token.symbol == ')':
        raise self.error('XPST0017', 'expected exactly one argument')
    self[0:] = self.parser.expression(5),
    if self.parser.next_token.symbol != ')':
        raise self.error('XPST0017', 'expected exactly one argument')
    self.parser.advance()
    raise self.error('XPST0017', "no constructor function exists for xs:NOTATION")


###
# Multirole tokens (function or constructor function)
#

# Case 1: In XPath 2.0 the 'boolean' keyword is used both for boolean() function and
# for boolean() constructor function.
unregister('boolean')


@constructor('boolean', label=('function', 'constructor function'),
             sequence_types=('item()*', 'xs:boolean'))
def cast__boolean_type(self: XPathConstructor, value: ta.AtomicType) -> bool:
    try:
        return cast(bool, BooleanProxy(value))
    except ValueError as err:
        raise self.error('FORG0001', err) from None
    except TypeError as err:
        raise self.error('XPTY0004', err) from None


@method('boolean')
def nud__boolean_type_and_function(self: XPathConstructor) -> XPathConstructor:
    if not self.parser.parse_arguments:
        return self

    self.parser.advance('(')
    if self.parser.next_token.symbol == ')':
        msg = 'Too few arguments: expected at least 1 argument'
        raise self.error('XPST0017', msg)
    self[0:] = self.parser.expression(5),
    if self.parser.next_token.symbol == ',':
        msg = 'Too many arguments: expected at most 1 argument'
        raise self.error('XPST0017', msg)
    self.parser.advance(')')
    return self


@method('boolean')
def evaluate__boolean_type_and_function(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[bool]:
    if self.context is not None:
        context = self.context

    if self.label == 'function':
        return self.boolean_value(self[0].select(context))

    # xs:boolean constructor
    arg = self.data_value(self.get_argument(context))
    if arg is None:
        return []

    try:
        return cast(bool, self.cast(arg))
    except ElementPathError as err:
        if isinstance(context, XPathSchemaContext):
            return []
        err.token = self
        raise


###
# Case 2: In XPath 2.0 the 'string' keyword is used both for fn:string() and xs:string().
unregister('string')


@constructor('string', label=('function', 'constructor function'),
             nargs=(0, 1), sequence_types=('item()?', 'xs:string'))
def cast__string_type(self: XPathConstructor, value: ta.AtomicType) -> str:
    return self.string_value(value)


@method('string')
def nud__string_type_and_function(self: XPathConstructor) -> XPathConstructor:
    if not self.parser.parse_arguments:
        return self

    try:
        self.parser.advance('(')
        if self.label != 'function' or self.parser.next_token.symbol != ')':
            self[0:] = self.parser.expression(5),
        self.parser.advance(')')
    except ElementPathSyntaxError as err:
        raise self.error('XPST0017', err)
    else:
        return self


@method('string')
def evaluate__string_type_and_function(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[str]:
    if self.context is not None:
        context = self.context

    if self.label == 'function':
        if not self:
            if context is None:
                raise self.missing_context()
            return self.string_value(context.item)
        return self.string_value(self.get_argument(context))
    else:
        item = self.get_argument(context)
        return [] if item is None else self.string_value(item)


# Case 3 and 4: In XPath 2.0 the XSD 'QName' and 'dateTime' types have special
# constructor functions so the 'QName' keyword is used both for fn:QName() and
# xs:QName(), the same for 'dateTime' keyword.
#
# In those cases the label at parse time is set by the nud method, in dependence
# of the number of args.
#
@constructor('QName', bp=90, label=('function', 'constructor function'),
             nargs=(1, 2), sequence_types=('xs:string?', 'xs:string', 'xs:QName'))
def cast__qname_type(self: XPathConstructor, value: str) -> QName:
    return self.cast_to_qname(value)


@constructor('dateTime', bp=90, label=('function', 'constructor function'),
             nargs=(1, 2), sequence_types=('xs:date?', 'xs:time?', 'xs:dateTime?'))
def cast__datetime_type(self: XPathConstructor, value: ta.AtomicType) -> DateTime | None:
    try:
        result = cast(DateTime, self.type_class).make(value, parser=self.parser)
    except OverflowError as err:
        raise self.error('FODT0001', err) from None
    except ValueError as err:
        raise self.error('FORG0001', err) from None
    else:
        return result


@method('QName')
@method('dateTime')
def nud__qname_and_datetime(self: XPathConstructor) -> XPathConstructor:
    if not self.parser.parse_arguments:
        return self

    try:
        self.parser.advance('(')
        self[0:] = self.parser.expression(5),
        if self.parser.next_token.symbol == ',':
            if self.label != 'function':
                raise self.error('XPST0017', 'unexpected 2nd argument')
            self.label = 'function'
            self.parser.advance(',')
            self[1:] = self.parser.expression(5),
        elif self.label != 'constructor function' or self.namespace != XSD_NAMESPACE:
            raise self.error('XPST0017', '2nd argument missing')
        else:
            self.label = 'constructor function'
            self.nargs = 1
        self.parser.advance(')')
    except SyntaxError:
        raise self.error('XPST0017') from None
    else:
        return self


@method('QName')
def evaluate__qname_type_and_function(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[QName]:
    if self.context is not None:
        context = self.context

    if self.label == 'constructor function':
        arg = self.data_value(self.get_argument(context))
        if arg is None:
            return []
        value = self.cast(arg)
        assert isinstance(value, QName)
        return value
    else:
        uri = self.get_argument(context)
        qname = self.get_argument(context, index=1)
        try:
            return QName(uri, qname)
        except (TypeError, ValueError) as err:
            if isinstance(context, XPathSchemaContext):
                return []
            elif isinstance(err, TypeError):
                raise self.error('XPTY0004', err)
            else:
                raise self.error('FOCA0002', err)


@method('dateTime')
def evaluate__datetime_type_and_function(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[DateTime]:
    if self.context is not None:
        context = self.context

    if self.label == 'constructor function':
        arg = self.data_value(self.get_argument(context))
        if arg is None:
            return []

        try:
            result = self.cast(arg)
        except (ValueError, TypeError) as err:
            if isinstance(context, XPathSchemaContext):
                return []
            elif isinstance(err, ValueError):
                raise self.error('FORG0001', err) from None
            else:
                raise self.error('FORG0006', err) from None
        else:
            assert isinstance(result, DateTime)
            return result
    else:
        dt = self.get_argument(context, cls=Date)
        tm = self.get_argument(context, 1, cls=Time)
        if dt is None or tm is None:
            return []
        elif dt.tzinfo == tm.tzinfo or tm.tzinfo is None:
            tzinfo = dt.tzinfo
        elif dt.tzinfo is None:
            tzinfo = tm.tzinfo
        else:
            raise self.error('FORG0008')

        cls = DateTime if self.parser.xsd_version == '1.1' else DateTime10
        return cls(dt.year, dt.month, dt.day, tm.hour, tm.minute,
                   tm.second, tm.microsecond, tzinfo)


@constructor('untypedAtomic')
def cast__untyped_atomic(self: XPathConstructor, value: ta.AtomicType) -> UntypedAtomic:
    return UntypedAtomic(value)


@method('untypedAtomic')
def evaluate__untyped_atomic(self: XPathConstructor, context: ta.ContextType = None) \
        -> ta.OneOrEmpty[UntypedAtomic]:
    arg = self.data_value(self.get_argument(self.context or context))
    if arg is None:
        return []
    elif isinstance(arg, UntypedAtomic):
        return arg
    else:
        arg = self.cast(arg)
        assert isinstance(arg, UntypedAtomic)
        return arg


###
# The fn:error function and the xs:error constructor.
#
# https://www.w3.org/TR/2010/REC-xpath-functions-20101214/#func-error
# https://www.w3.org/TR/xpath-functions/#func-error
#
#

# TODO: apply sequence_types=('xs:anyAtomicType?', 'xs:error?') for xs:error
@constructor('error', bp=90, label=('function', 'constructor function'), nargs=(0, 3),
             sequence_types=('xs:QName?', 'xs:string', 'item()*', 'none'))
def cast__error_type(self: XPathConstructor, value: ta.AtomicType) -> list[NoReturn]:
    try:
        result = ErrorProxy(value)
    except (TypeError, ValueError) as err:
        raise self.error('FORG0001', str(err)) from None
    else:
        if result is None:
            return []
        elif not isinstance(result, list) or len(result):
            raise self.error('FORG0001')
        return result


@method('error')
def nud__error_type_and_function(self: XPathConstructor) -> XPathConstructor:
    self.clear()
    if not self.parser.parse_arguments:
        return self

    try:
        self.parser.advance('(')
        if self.namespace == XSD_NAMESPACE:
            self.label = 'constructor function'
            self.nargs = 1
            if self.parser.xsd_version == '1.0':
                raise self.error('XPST0051', 'xs:error is not defined with XSD 1.0')
            self.append(self.parser.expression(5))
        else:
            self.label = 'function'
            for k in range(3):
                if self.parser.next_token.symbol == ')':
                    break
                self.append(self.parser.expression(5))
                if self.parser.next_token.symbol == ')':
                    break
                self.parser.advance(',')
        self.parser.advance(')')
    except SyntaxError:
        raise self.error('XPST0017') from None
    else:
        return self


@method('error')
def evaluate__error_type_and_function(self: XPathConstructor,
                                      context: ta.ContextType = None) -> None:
    if self.context is not None:
        context = self.context

    error: QName | None
    if self.label == 'constructor function':
        self.cast(self.get_argument(context))
    elif not self:
        raise self.error('FOER0000')
    elif len(self) == 1:
        error = self.get_argument(context, cls=QName)
        if error is None and self.parser.version <= '3.0':
            raise self.error('XPTY0004', "an xs:QName expected")
        raise self.error(error or 'FOER0000')
    else:
        error = self.get_argument(context, cls=QName)
        description: str | None = self.get_argument(context, index=1, cls=str)
        raise self.error(error or 'FOER0000', description)


###
# XSD list-based constructors

@constructor('NMTOKENS', sequence_types=('xs:NMTOKEN*',))
def cast__nmtokens(self: XPathConstructor, value: ta.AtomicType) -> list[NMToken]:
    if isinstance(value, UntypedAtomic):
        values = value.value.split() or [value.value]
    elif hasattr(value, 'split'):
        values = value.split() or [value]
    else:
        raise self.error('FORG0001')

    try:
        return xlist([NMToken(x) for x in values])
    except ValueError as err:
        raise self.error('FORG0001', err) from None


@constructor('IDREFS', sequence_types=('xs:IDREF*',))
def cast__idrefs(self: XPathConstructor, value: ta.AtomicType) -> list[Idref]:
    if isinstance(value, UntypedAtomic):
        values = value.value.split() or [value.value]
    elif hasattr(value, 'split'):
        values = value.split() or [value]
    else:
        raise self.error('FORG0001')

    try:
        return xlist([Idref(x) for x in values])
    except ValueError as err:
        raise self.error('FORG0001', err) from None


@constructor('ENTITIES', sequence_types=('xs:ENTITY*',))
def cast__entities(self: XPathConstructor, value: ta.AtomicType) -> list[Entity]:
    if isinstance(value, UntypedAtomic):
        values = value.value.split() or [value.value]
    elif hasattr(value, 'split'):
        values = value.split() or [value]
    else:
        raise self.error('FORG0001')

    try:
        return xlist([Entity(x) for x in values])
    except ValueError as err:
        raise self.error('FORG0001', err) from None
