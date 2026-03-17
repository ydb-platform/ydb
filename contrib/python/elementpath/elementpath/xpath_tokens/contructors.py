#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from typing import Any

import elementpath.aliases as ta

from elementpath.exceptions import ElementPathError, MissingContextError
from elementpath.datatypes import AnyAtomicType, ListType, UntypedAtomic
from elementpath.xpath_context import XPathContext, XPathSchemaContext
from .functions import XPathFunction


class XPathConstructor(XPathFunction):
    """
    A token for processing XPath 2.0+ constructors.
    """
    type_class: type[AnyAtomicType | ListType]

    @staticmethod
    def cast(value: Any) -> ta.AtomicType:
        raise NotImplementedError()

    def nud(self) -> 'XPathConstructor':
        if not self.parser.parse_arguments:
            return self

        try:
            self.parser.advance('(')
            self[0:] = self.parser.expression(5),
            if self.parser.next_token.symbol == ',':
                msg = 'Too many arguments: expected at most 1 argument'
                raise self.error('XPST0017', msg)
            self.parser.advance(')')
        except SyntaxError:
            raise self.error('XPST0017') from None
        else:
            if self[0].symbol == '?':
                self.to_partial_function()
            return self

    def evaluate(self, context: XPathContext | None = None) -> ta.OneAtomicOrEmpty:
        if self.context is not None:
            context = self.context

        arg = self.data_value(self.get_argument(context))
        if arg is None:
            return []
        elif arg == '?' and self[0].symbol == '?':
            raise self.error('XPTY0004', "cannot evaluate a partial function")

        try:
            if isinstance(arg, UntypedAtomic):
                return self.cast(arg.value)
            return self.cast(arg)
        except ElementPathError:
            raise
        except (TypeError, ValueError) as err:
            if isinstance(context, XPathSchemaContext):
                return []
            raise self.error('FORG0001', err) from None


class SchemaConstructor(XPathFunction):
    """
    A token for processing XPath 2.0+ constructors defined by schema atomic types.
    """
    type_class: type[AnyAtomicType | ListType]

    @staticmethod
    def cast(value: Any) -> ta.AtomicType:
        raise NotImplementedError()

    def nud(self) -> 'SchemaConstructor':
        self.parser.advance('(')
        self[0:] = self.parser.expression(5),
        self.parser.advance(')')

        try:
            self.evaluate()  # for static context evaluation
        except MissingContextError:
            pass
        return self

    def evaluate(self, context: ta.ContextType = None) -> ta.OneAtomicOrEmpty:
        arg = self.get_argument(context)
        if arg is None or self.parser.schema is None:
            return []

        value = self.string_value(arg)
        try:
            return self.parser.schema.cast_as(value, self.name)
        except (TypeError, ValueError) as err:
            if isinstance(context, XPathSchemaContext):
                return []
            raise self.error('FORG0001', err)
