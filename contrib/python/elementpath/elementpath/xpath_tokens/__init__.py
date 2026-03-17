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
XPathToken class and derived classes for other XPath objects (functions, constructors,
axes, maps, arrays). XPath's error creation and node helper functions are embedded in
XPathToken class, in order to raise errors related to token instances.
"""
from dataclasses import dataclass
from typing import Any

from elementpath.datatypes import UntypedAtomic, QName

from .base import XPathToken
from .axes import XPathAxis
from .functions import XPathFunction, ExternalFunction
from .contructors import XPathConstructor, SchemaConstructor
from .maps import XPathMap
from .arrays import XPathArray
from .tokens import ValueToken, ProxyToken, NameToken, PrefixedNameToken, \
    BracedNameToken, VariableToken, AsteriskToken, ParentShortcutToken, \
    ContextItemToken

__all__ = ['XPathToken', 'XPathAxis', 'XPathFunction', 'XPathConstructor',
           'XPathMap', 'XPathArray', 'ValueToken', 'ProxyToken', 'NameToken',
           'PrefixedNameToken', 'BracedNameToken', 'VariableToken', 'AsteriskToken',
           'ParentShortcutToken', 'ContextItemToken', 'SchemaConstructor',
           'ExternalFunction', 'TokenRegistry']


@dataclass(frozen=True, slots=True)
class TokenRegistry:
    """A registry of classes, helpers and instances used commonly by XPath tokens."""

    # Token base classes
    base_token: type[XPathToken] = XPathToken
    axis_token: type[XPathAxis] = XPathAxis
    function_token: type[XPathFunction] = XPathFunction
    external_function_token: type[ExternalFunction] = ExternalFunction
    constructor_token: type[XPathConstructor] = XPathConstructor
    schema_constructor_token: type[SchemaConstructor] = SchemaConstructor
    array_token: type[XPathArray] = XPathArray
    map_token: type[XPathMap] = XPathMap

    # Other token common classes
    value_token: type[ValueToken] = ValueToken
    proxy_token: type[ProxyToken] = ProxyToken
    name_token: type[NameToken] = NameToken
    prefixed_name_token: type[PrefixedNameToken] = PrefixedNameToken
    braced_name_token: type[BracedNameToken] = BracedNameToken
    variable_token: type[VariableToken] = VariableToken
    asterisk_token: type[AsteriskToken] = AsteriskToken
    parent_shortcut_token: type[ParentShortcutToken] = ParentShortcutToken
    context_item_token: type[ContextItemToken] = ContextItemToken

    QName: type[QName] = QName
    UntypedAtomic: type[UntypedAtomic] = UntypedAtomic

    class __Name:
        name: str | None = None

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self.__Name.name = name

    def __set__(self, instance: Any, value: Any) -> None:
        raise AttributeError("Can't set attribute {!r}".format(self.__Name.name))

    def __delete__(self, instance: Any) -> None:
        raise AttributeError("Can't delete attribute {!r}".format(self.__Name.name))


XPathToken.registry = TokenRegistry()
