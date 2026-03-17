#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""Helper functions for QNames and namespaces."""
import re
from collections.abc import Iterable, MutableMapping
from functools import cache

from xmlschema.exceptions import XMLSchemaValueError, XMLSchemaTypeError
from xmlschema.names import XML_NAMESPACE
from xmlschema.aliases import NsmapType


def get_namespace(qname: str) -> str:
    """
    Returns the namespace URI associated with a QName in extended form or a local name.
    If the argument is not conformant to QName format returns the empty string, which
    means no namespace.
    """
    try:
        if qname[0] != '{':
            return ''
        namespace, _ = qname[1:].split('}')
    except (IndexError, ValueError):
        return ''
    except TypeError:
        raise XMLSchemaTypeError("the argument must be a string-like object")
    else:
        return namespace


def get_namespace_ext(qname: str, namespaces: NsmapType | None = None) -> str:
    """
    Returns the namespace URI associated with a QName. If a namespace map is
    provided tries to resolve a prefixed QName and then to extract the namespace.

    :param qname: an extended QName or a local name or a prefixed QName.
    :param namespaces: optional mapping from prefixes to namespace URIs.
    """
    if not namespaces:
        return get_namespace(qname)
    else:
        return get_namespace(get_extended_qname(qname, namespaces))


@cache
def get_qname(uri: str | None, name: str) -> str:
    """
    Returns an expanded QName from URI and local part. If any argument has boolean value
    `False` or if the name is already an expanded QName, returns the *name* argument.

    :param uri: namespace URI
    :param name: local or qualified name
    :return: string or the name argument
    """
    try:
        if name[0] in '{./[' or not uri:
            return name
    except IndexError:
        return ''
    except TypeError:
        raise XMLSchemaTypeError("the 2nd argument must be a string-like object")
    else:
        return f'{{{uri}}}{name}'


@cache
def local_name(qname: str) -> str:
    """
    Return the local part of an expanded QName or a prefixed name. If the name
    is `None` or empty returns the *name* argument.

    :param qname: an expanded QName or a prefixed name or a local name.
    """
    try:
        if qname[0] == '{':
            _namespace, qname = qname.split('}')
        elif ':' in qname:
            _prefix, qname = qname.split(':')
    except IndexError:
        return ''
    except ValueError:
        raise XMLSchemaValueError("the argument 'qname' has an invalid value %r" % qname)
    except TypeError:
        raise XMLSchemaTypeError("the argument 'qname' must be a string-like object")
    else:
        return qname


def get_prefixed_qname(qname: str,
                       namespaces: MutableMapping[str, str] | None,
                       use_empty: bool = True) -> str:
    """
    Get the prefixed form of a QName, using a namespace map.

    :param qname: an extended QName or a local name or a prefixed QName.
    :param namespaces: an optional mapping from prefixes to namespace URIs.
    :param use_empty: if `True` use the empty prefix for mapping.
    """
    if not namespaces or not qname or qname[0] != '{':
        return qname

    namespace = get_namespace(qname)
    prefixes = [x for x in namespaces if namespaces[x] == namespace]

    if not prefixes:
        return qname
    elif prefixes[0]:
        return f"{prefixes[0]}:{qname.split('}', 1)[1]}"
    elif len(prefixes) > 1:
        return f"{prefixes[1]}:{qname.split('}', 1)[1]}"
    elif use_empty:
        return qname.split('}', 1)[1]
    else:
        return qname


def get_extended_qname(qname: str, namespaces: MutableMapping[str, str] | None) -> str:
    """
    Get the extended form of a QName, using a namespace map.
    Local names are mapped to the default namespace.

    :param qname: a prefixed QName or a local name or an extended QName.
    :param namespaces: an optional mapping from prefixes to namespace URIs.
    """
    if not namespaces:
        return qname

    try:
        if qname[0] == '{':
            return qname
    except IndexError:
        return qname

    try:
        prefix, name = qname.split(':', 1)
    except ValueError:
        if not namespaces.get(''):
            return qname
        else:
            return f"{{{namespaces['']}}}{qname}"
    else:
        try:
            uri = namespaces[prefix]
        except KeyError:
            return qname
        else:
            return f'{{{uri}}}{name}' if uri else name


def update_namespaces(namespaces: dict[str, str],
                      xmlns: Iterable[tuple[str, str]],
                      root_declarations: bool = False) -> None:
    """
    Update a namespace map without overwriting existing declarations.
    If a duplicate prefix is encountered in a xmlns declaration, and
    this is mapped to a different namespace, adds the namespace using
    a different generated prefix. The empty prefix '' is used only if
    it's declared at root level to avoid erroneous mapping of local
    names. In other cases it uses the prefix 'default' as substitute.

    :param namespaces: the target namespace map.
    :param xmlns: an iterable containing couples of namespace declarations.
    :param root_declarations: provide `True` if the namespace declarations \
    belong to the root element, `False` otherwise (default).
    """
    for prefix, uri in xmlns:
        if not prefix:
            if not uri:
                continue
            elif '' not in namespaces:
                if root_declarations:
                    namespaces[''] = uri
                    continue
            elif namespaces[''] == uri:
                continue
            prefix = 'default'

        while prefix in namespaces:
            if namespaces[prefix] == uri:
                break
            match = re.search(r'(\d+)$', prefix)
            if match:
                index = int(match.group()) + 1
                prefix = prefix[:match.span()[0]] + str(index)
            else:
                prefix += '0'
        else:
            namespaces[prefix] = uri


def get_namespace_map(namespaces: NsmapType | None) -> dict[str, str]:
    """Returns a new and checked namespace map."""
    if namespaces is None:
        return {}

    namespaces = {k: v for k, v in namespaces.items()}
    if namespaces.get('xml', XML_NAMESPACE) != XML_NAMESPACE:
        msg = f"reserved prefix 'xml' can be used only for {XML_NAMESPACE!r} namespace"
        raise XMLSchemaValueError(msg)

    return namespaces
