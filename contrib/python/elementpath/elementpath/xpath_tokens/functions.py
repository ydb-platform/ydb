#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from collections.abc import Callable, Iterator
from copy import copy
from typing import Optional, cast, Union, Any

import elementpath.aliases as ta

from elementpath.protocols import ElementProtocol, DocumentProtocol
from elementpath.datatypes import QName, AnyAtomicType
from elementpath.tdop import MultiLabel
from elementpath.namespaces import XSD_NAMESPACE, XPATH_FUNCTIONS_NAMESPACE, \
    XPATH_MATH_FUNCTIONS_NAMESPACE
from elementpath.helpers import split_function_test, ordinal
from elementpath.sequences import xlist
from elementpath.etree import is_etree_document, is_etree_element
from elementpath.sequence_types import match_sequence_type, is_sequence_type_restriction
from elementpath.xpath_nodes import XPathNode
from elementpath.tree_builders import get_node_tree

from .base import XPathToken
from .tokens import ValueToken


class XPathFunction(XPathToken):
    """
    A token for processing XPath functions.
    """
    __name__: str
    _qname: Optional[QName] = None
    pattern = r'(?<!\$)\b[^\d\W][\w.\-\xb7\u0300-\u036F\u203F\u2040]*' \
              r'(?=\s*(?:\(\:.*\:\))?\s*\((?!\:))'

    sequence_types: ta.SequenceTypesType = ()
    "Sequence types of arguments and of the return value of the function."

    nargs: ta.NargsType = None
    "Number of arguments: a single value or a couple with None that means unbounded."

    context: ta.ContextType = None
    "Dynamic context associated by function reference evaluation or explicitly by a builder."

    def __init__(self, parser: ta.XPathParserType, nargs: Optional[int] = None) -> None:
        super().__init__(parser)
        if isinstance(nargs, int) and nargs != self.nargs:
            if nargs < 0:
                raise self.error('XPST0017', 'number of arguments must be non negative')
            elif self.nargs is None:
                self.nargs = nargs
            elif isinstance(self.nargs, int):
                raise self.error('XPST0017', 'incongruent number of arguments')
            elif self.nargs[0] > nargs or self.nargs[1] is not None and self.nargs[1] < nargs:
                raise self.error('XPST0017', 'incongruent number of arguments')
            else:
                self.nargs = nargs

    def __repr__(self) -> str:
        qname = self.qname
        if qname is None:
            return '<%s object at %#x>' % (self.__class__.__name__, id(self))
        elif not isinstance(self.nargs, int):
            return '<XPathFunction %s at %#x>' % (qname.qname, id(self))
        return '<XPathFunction %s#%r at %#x>' % (qname.qname, self.nargs, id(self))

    def __str__(self) -> str:
        if self.namespace is None:
            return f'{self.symbol!r} {self.label}'
        elif self.namespace == XPATH_FUNCTIONS_NAMESPACE:
            return f"'fn:{self.symbol}' {self.label}"
        else:
            for prefix, uri in self.parser.namespaces.items():
                if uri == self.namespace:
                    return f"'{prefix}:{self.symbol}' {self.label}"
            else:
                return f"'Q{{{self.namespace}}}{self.symbol}' {self.label}"

    def __call__(self, *args: ta.FunctionArgType, context: ta.ContextType = None) -> ta.ValueType:
        self.check_arguments_number(len(args))
        context = copy(self.context or context)

        if self.label == 'partial function':
            for arg, tk in zip(args, filter(lambda x: x.symbol == '?', self)):
                if isinstance(arg, XPathFunction) or not isinstance(arg, XPathToken):
                    tk.value = self.validated_argument(arg, context)
                else:
                    tk.value = arg.evaluate(context)
        else:
            self.clear()
            for arg in args:
                if isinstance(arg, XPathToken):
                    self._items.append(arg)
                else:
                    value = self.validated_argument(arg, context)
                    # Accepts and wraps etree elements/documents, useful for external calls.
                    self._items.append(ValueToken(self.parser, value=value))

            if any(tk.symbol == '?' and not tk for tk in self._items):
                self.to_partial_function()
                return self

        if isinstance(self.label, MultiLabel):
            # Disambiguate multi-label tokens
            if self.namespace == XSD_NAMESPACE and \
                    'constructor function' in self.label.values:
                self.label = 'constructor function'
            else:
                for label in self.label.values:
                    if label.endswith('function'):
                        self.label = label
                        break

        if self.label == 'partial function':
            result = self._partial_evaluate(context)
        else:
            result = self.evaluate(context)

        return self.validated_result(result)

    def check_arguments_number(self, nargs: int) -> None:
        """Check the number of arguments against function arity."""
        if self.nargs is None or self.nargs == nargs:
            pass
        elif isinstance(self.nargs, tuple):
            if nargs < self.nargs[0]:
                raise self.error('XPTY0004', "missing required arguments")
            elif self.nargs[1] is not None and nargs > self.nargs[1]:
                raise self.error('XPTY0004', "too many arguments")
        elif self.nargs > nargs:
            raise self.error('XPTY0004', "missing required arguments")
        else:
            raise self.error('XPTY0004', "too many arguments")

    def validated_argument(self, arg: ta.FunctionArgType,
                           context: ta.ContextType = None) -> ta.ValueType:

        def get_arg_item(item: ta.FunctionArgType) -> ta.ItemType:
            if isinstance(item, (XPathNode, XPathFunction, AnyAtomicType)):
                return item
            elif not is_etree_document(item) and not is_etree_element(item):
                raise self.error('XPTY0004', f"unexpected argument type {type(item)}")
            else:
                return get_node_tree(
                    cast(Union[ElementProtocol, DocumentProtocol], item),
                    namespaces=self.parser.namespaces,
                    uri=self.parser.base_uri,
                    fragment=None
                )

        if context is not None:
            return context.get_value(arg, context.namespaces, self.parser.base_uri, None)
        elif arg is None:
            return []
        elif not isinstance(arg, (list, tuple)):
            return get_arg_item(arg)
        return [get_arg_item(x) for x in arg]

    def validated_result(self, result: ta.ValueType) -> ta.ValueType:
        if isinstance(result, XPathToken) and result.symbol == '?':
            return result
        elif match_sequence_type(result, self.sequence_types[-1], self.parser):
            return result

        result = self.cast_to_primitive_type(result, self.sequence_types[-1])
        if not match_sequence_type(result, self.sequence_types[-1], self.parser):
            msg = "{!r} does not match sequence type {}"
            raise self.error('XPTY0004', msg.format(result, self.sequence_types[-1]))
        return result

    @property
    def source(self) -> str:
        return f"{self.symbol}({', '.join(t.source for t in self)}){self.occurrence}"

    @property
    def qname(self) -> Optional[QName]:
        if self._qname is not None:
            return self._qname
        elif self.symbol == 'function':
            return None
        elif self.label == 'partial function':
            return None
        elif not self.namespace:
            self._qname = QName(None, self.symbol)
        elif self.namespace == XPATH_FUNCTIONS_NAMESPACE:
            self._qname = QName(XPATH_FUNCTIONS_NAMESPACE, 'fn:%s' % self.symbol)
        elif self.namespace == XSD_NAMESPACE:
            self._qname = QName(XSD_NAMESPACE, 'xs:%s' % self.symbol)
        elif self.namespace == XPATH_MATH_FUNCTIONS_NAMESPACE:
            self._qname = QName(XPATH_MATH_FUNCTIONS_NAMESPACE, 'math:%s' % self.symbol)
        else:
            for pfx, uri in self.parser.namespaces.items():
                if uri == self.namespace:
                    self._qname = QName(uri, f'{pfx}:{self.symbol}')
                    break
            else:
                self._qname = QName(self.namespace, self.symbol)

        return self._qname

    @property
    def arity(self) -> int:
        if isinstance(self.nargs, int):
            return self.nargs
        return len(self._items)

    @property
    def min_args(self) -> int:
        if isinstance(self.nargs, int):
            return self.nargs
        elif isinstance(self.nargs, (tuple, list)):
            return self.nargs[0]
        else:
            return 0

    @property
    def max_args(self) -> Optional[int]:
        if isinstance(self.nargs, int):
            return self.nargs
        elif isinstance(self.nargs, (tuple, list)):
            return self.nargs[1]
        else:
            return None

    def is_reference(self) -> int:
        if not isinstance(self.nargs, int):
            return False
        return self.nargs and not len(self._items)

    def nud(self) -> 'XPathFunction':
        if not self.parser.parse_arguments:
            return self

        code = 'XPST0017' if 'function' in self.label else 'XPST0003'
        self.parser.advance('(')
        if self.nargs is None:
            del self._items[:]
            if self.parser.next_token.symbol in (')', '(end)'):
                raise self.error(code, 'at least an argument is required')
            while True:
                self.append(self.parser.expression(5))
                if self.parser.next_token.symbol != ',':
                    break
                self.parser.advance()
        elif self.nargs == 0:
            if self.parser.next_token.symbol != ')':
                if self.parser.next_token.symbol != '(end)':
                    raise self.error(code, '%s has no arguments' % str(self))
                raise self.parser.next_token.wrong_syntax()
            self.parser.advance()
            return self
        else:
            if isinstance(self.nargs, (tuple, list)):
                min_args, max_args = self.nargs
            else:
                min_args = max_args = self.nargs

            k = 0
            while k < min_args:
                if self.parser.next_token.symbol in (')', '(end)'):
                    msg = 'Too few arguments: expected at least %s arguments' % min_args
                    raise self.error('XPST0017', msg if min_args > 1 else msg[:-1])

                self._items[k:] = self.parser.expression(5),
                k += 1
                if k < min_args:
                    if self.parser.next_token.symbol == ')':
                        msg = f'{str(self)}: Too few arguments, expected ' \
                              f'at least {min_args} arguments'
                        raise self.error(code, msg if min_args > 1 else msg[:-1])
                    self.parser.advance(',')

            while max_args is None or k < max_args:
                if self.parser.next_token.symbol == ',':
                    self.parser.advance(',')
                    self._items[k:] = self.parser.expression(5),
                elif k == 0 and self.parser.next_token.symbol != ')':
                    self._items[k:] = self.parser.expression(5),
                else:
                    break  # pragma: no cover
                k += 1

            if self.parser.next_token.symbol == ',':
                msg = 'Too many arguments: expected at most %s arguments' % max_args
                raise self.error(code, msg if max_args != 1 else msg[:-1])

        self.parser.advance(')')
        if any(tk.symbol == '?' and not tk for tk in self._items):
            self.to_partial_function()

        return self

    def match_function_test(self, function_test: ta.SequenceTypesType,
                            as_argument: bool = False) -> bool:
        """
        Match if function signature satisfies the provided *function_test*.
        For default return type is covariant and arguments are contravariant.
        If *as_argument* is `True` the match is inverted.

        References:
          https://www.w3.org/TR/xpath-31/#id-function-test
          https://www.w3.org/TR/xpath-31/#id-sequencetype-subtype
        """
        if isinstance(function_test, (list, tuple)):
            sequence_types = function_test
        else:
            sequence_types = split_function_test(function_test)
        if not sequence_types or not sequence_types[-1]:
            return False
        elif sequence_types[0] == '*':
            return True

        signature = [x for x in self.sequence_types[:self.arity]]
        signature.append(self.sequence_types[-1])

        if len(sequence_types) != len(signature):
            return False

        if as_argument:
            iterator = zip(sequence_types[:-1], signature[:-1])
        else:
            iterator = zip(signature[:-1], sequence_types[:-1])

        # compare sequence types
        for st1, st2 in iterator:
            if not is_sequence_type_restriction(st1, st2):
                return False
        else:
            st1, st2 = sequence_types[-1], signature[-1]
            return is_sequence_type_restriction(st1, st2)

    def to_partial_function(self) -> None:
        """Convert an XPath function to a partial function."""
        nargs = len([tk and not tk for tk in self._items if tk.symbol == '?'])
        assert nargs, "a partial function requires at least a placeholder token"

        if self.label != 'partial function':
            def evaluate(context: ta.ContextType = None) -> 'XPathFunction':
                return self

            def select(context: ta.ContextType = None) -> Iterator['XPathFunction']:
                yield self

            if self.__class__.evaluate is not XPathToken.evaluate:
                setattr(self, '_partial_evaluate', self.evaluate)
            if self.__class__.select is not XPathToken.select:
                setattr(self, '_partial_select', self.select)

            setattr(self, 'evaluate', evaluate)
            setattr(self, 'select', select)

        self._qname = None
        self.label = 'partial function'
        self.nargs = nargs

    def as_function(self) -> Callable[..., Any]:
        """
        Wraps the XPath function instance into a standard function.
        """
        def wrapper(*args: ta.FunctionArgType, context: ta.ContextType = None) -> ta.ValueType:
            return self.__call__(*args, context=context)

        qname = self.qname
        if self.is_reference():
            ref_part = f'#{self.nargs}'
        else:
            ref_part = ''

        if qname is None:
            name = f'<anonymous-function{ref_part}>'
        else:
            name = f'<{qname.qname}{ref_part}>'

        wrapper.__name__ = name
        wrapper.__qualname__ = wrapper.__qualname__[:-7] + name
        return wrapper

    def _partial_evaluate(self, context: ta.ContextType = None) -> ta.ValueType:
        return xlist(self._partial_select(context))

    def _partial_select(self, context: ta.ContextType = None) -> Iterator[ta.ItemType]:
        item = self._partial_evaluate(context)
        if item is not None:
            if isinstance(item, list):
                yield from item
            else:
                if context is not None:
                    context.item = item
                yield item


class ExternalFunction(XPathFunction):
    callback: Callable[..., Any]

    def evaluate(self, context: ta.ContextType = None) -> Any:
        args = []
        for k in range(len(self)):
            try:
                if self.sequence_types[k][-1] in '+*':
                    arg = self[k].evaluate(context)
                else:
                    arg = self.get_argument(context, index=k)
            except IndexError:
                arg = self.get_argument(context, index=k)
            args.append(arg)

        if self.sequence_types:
            for k, (arg, st) in enumerate(zip(args, self.sequence_types), start=1):
                if not match_sequence_type(arg, st, self.parser):
                    msg = f"{ordinal(k)} argument does not match sequence type {st!r}"
                    raise self.error('XPDY0050', msg)

            result = self.callback(*args)
            if not match_sequence_type(result, self.sequence_types[-1], self.parser):
                msg = f"Result does not match sequence type {self.sequence_types[-1]!r}"
                raise self.error('XPDY0050', msg)
            return result

        return self.callback(*args)
