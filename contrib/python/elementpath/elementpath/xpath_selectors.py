#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import datetime
import warnings
from collections.abc import Iterator
from typing import Any, Optional, Union

from elementpath.aliases import NamespacesType, InputType, ParserClassType, \
    RootArgType, ItemArgType
from elementpath.xpath_context import XPathContext
from elementpath.xpath2 import XPath2Parser
from elementpath.datatypes import Timezone
from elementpath.schema_proxy import AbstractSchemaProxy


def select(root: Optional[RootArgType],
           path: str,
           namespaces: Optional[NamespacesType] = None,
           parser: Optional[ParserClassType] = None,
           uri: Optional[str] = None,
           fragment: Optional[bool] = None,
           item: Optional[ItemArgType] = None,
           position: int = 1,
           size: int = 1,
           axis: Optional[str] = None,
           schema: Optional[AbstractSchemaProxy] = None,
           variables: Optional[dict[str, InputType[ItemArgType]]] = None,
           current_dt: Optional[datetime.datetime] = None,
           timezone: Optional[Union[str, Timezone]] = None,
           **kwargs: Any) -> Any:
    """
    XPath selector function that apply a *path* expression on *root* Element.

    :param root: the root of the XML document, usually an ElementTree instance or an \
    Element. A schema or a schema element can also be provided, or an already built \
    node tree. You can also provide `None`, in which case no XML root node is set in \
    the dynamic context, and you have to provide the keyword argument *item*.
    :param path: the XPath expression.
    :param namespaces: a dictionary with mapping from namespace prefixes into URIs.
    :param parser: the parser class to use, that is :class:`XPath2Parser` for default.
    :param uri: an optional URI associated with the root element or the document.
    :param fragment: if `True` is provided the root is considered a fragment. In this \
    case if `root` is an ElementTree instance skips it and use the root Element. If \
    `False` is provided creates a dummy document when the root is an Element instance. \
    In this case the dummy document value is not included in results. For default the \
    root node kind is preserved.
    :param item: the context item. A `None` value means that the context is positioned on \
    the document node.
    :param position: the current position of the node within the input sequence.
    :param size: the number of items in the input sequence.
    :param axis: the active axis. Used to choose when apply the default axis ('child' axis).
    :param schema: an optional schema proxy instance for applying XSD type annotations \
    on element and attribute nodes.
    :param variables: dictionary of context variables that maps a QName to a value.
    :param current_dt: current dateTime of the implementation, including explicit timezone.
    :param timezone: implicit timezone to be used when a date, time, or dateTime value does \
    not have a timezone.
    :param kwargs: other optional parameters for the parser instance.
    :return: a list with XPath nodes or a basic type for expressions based \
    on a function or literal.
    """
    if parser is None:
        parser = XPath2Parser
    if schema is not None and parser.version > '1.0':
        kwargs['schema'] = schema

    root_token = parser(namespaces, **kwargs).parse(path)
    context = XPathContext(root, namespaces, uri, fragment, item, position, size,
                           axis, schema, variables, current_dt, timezone)
    return root_token.get_results(context)


def iter_select(root: Optional[RootArgType],
                path: str,
                namespaces: Optional[NamespacesType] = None,
                parser: Optional[ParserClassType] = None,
                uri: Optional[str] = None,
                fragment: Optional[bool] = None,
                item: Optional[ItemArgType] = None,
                position: int = 1,
                size: int = 1,
                axis: Optional[str] = None,
                schema: Optional[AbstractSchemaProxy] = None,
                variables: Optional[dict[str, InputType[ItemArgType]]] = None,
                current_dt: Optional[datetime.datetime] = None,
                timezone: Optional[Union[str, Timezone]] = None,
                **kwargs: Any) -> Iterator[Any]:
    """
    A function that creates an XPath selector generator for apply a *path* expression
    on *root* Element.

    :param root: the root of the XML document, usually an ElementTree instance or an \
    Element. A schema or a schema element can also be provided, or an already built \
    node tree. You can also provide `None`, in which case no XML root node is set in \
    the dynamic context, and you have to provide the keyword argument *item*.
    :param path: the XPath expression.
    :param namespaces: a dictionary with mapping from namespace prefixes into URIs.
    :param parser: the parser class to use, that is :class:`XPath2Parser` for default.
    :param uri: an optional URI associated with the root element or the document.
    :param fragment: if `True` is provided the root is considered a fragment. In this \
    case if `root` is an ElementTree instance skips it and use the root Element. If \
    `False` is provided creates a dummy document when the root is an Element instance. \
    In this case the dummy document value is not included in results. For default the \
    root node kind is preserved.
    :param item: the context item. A `None` value means that the context is positioned on \
    the document node.
    :param position: the current position of the node within the input sequence.
    :param size: the number of items in the input sequence.
    :param axis: the active axis. Used to choose when apply the default axis ('child' axis).
    :param schema: an optional schema proxy instance for applying XSD type annotations \
    on element and attribute nodes.
    :param variables: dictionary of context variables that maps a QName to a value.
    :param current_dt: current dateTime of the implementation, including explicit timezone.
    :param timezone: implicit timezone to be used when a date, time, or dateTime value does \
    not have a timezone.
    :param kwargs: other optional parameters for the parser instance.
    :return: a generator of the XPath expression results.
    """
    if parser is None:
        parser = XPath2Parser
    if schema is not None and parser.version > '1.0':
        kwargs['schema'] = schema

    root_token = parser(namespaces, **kwargs).parse(path)
    context = XPathContext(root, namespaces, uri, fragment, item, position, size,
                           axis, schema, variables, current_dt, timezone)
    return root_token.select_results(context)


class Selector(object):
    """
    XPath selector class. Create an instance of this class if you want to apply an XPath
    selector to several target data.

    :param path: the XPath expression.
    :param namespaces: a dictionary with mapping from namespace prefixes into URIs.
    :param parser: the parser class to use, that is :class:`XPath2Parser` for default.
    :param kwargs: other optional parameters for the XPath parser instance.

    :ivar path: the XPath expression.
    :vartype path: str
    :ivar parser: the parser instance.
    :vartype parser: XPath1Parser or XPath2Parser
    :ivar root_token: the root of tokens tree compiled from path.
    :vartype root_token: XPathToken
    """
    def __init__(self, path: str,
                 namespaces: Optional[NamespacesType] = None,
                 parser: Optional[ParserClassType] = None,
                 **kwargs: Any) -> None:

        if 'variables' in kwargs:
            msg = ("Argument 'variables' here is deprecated and will be"
                   "be removed in the next major release. Provide this "
                   "argument later to Selector.select/iter_select.")
            warnings.warn(DeprecationWarning(msg))

        self._variables = kwargs.pop('variables', None)
        self.parser = (parser or XPath2Parser)(namespaces, **kwargs)
        self.path = path
        self.root_token = self.parser.parse(path)

    def __repr__(self) -> str:
        return '%s(path=%r, parser=%s)' % (
            self.__class__.__name__, self.path, self.parser.__class__.__name__
        )

    @property
    def namespaces(self) -> dict[str, str]:
        """A dictionary with mapping from namespace prefixes into URIs."""
        return self.parser.namespaces

    def select(self, root: Optional[RootArgType], **kwargs: Any) -> Any:
        """
        Applies the instance's XPath expression on *root* Element.

        :param root: the root of the XML document, usually an ElementTree instance \
        or an Element.
        :param kwargs: other optional parameters for the XPath dynamic context.
        :return: a list with XPath nodes or a basic type for expressions based on \
        a function or literal.
        """
        if 'schema' not in kwargs:
            kwargs['schema'] = self.parser.schema
        if 'variables' not in kwargs and self._variables:
            kwargs['variables'] = self._variables

        context = XPathContext(root, **kwargs)
        return self.root_token.get_results(context)

    def iter_select(self, root: Optional[RootArgType], **kwargs: Any) -> Iterator[Any]:
        """
        Creates an XPath selector generator for apply the instance's XPath expression
        on *root* Element.

        :param root: the root of the XML document, usually an ElementTree instance \
        or an Element.
        :param kwargs: other optional parameters for the XPath dynamic context.
        :return: a generator of the XPath expression results.
        """
        if 'schema' not in kwargs:
            kwargs['schema'] = self.parser.schema
        if 'variables' not in kwargs and self._variables:
            kwargs['variables'] = self._variables

        context = XPathContext(root, **kwargs)
        return self.root_token.select_results(context)
