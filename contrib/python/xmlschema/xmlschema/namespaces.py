#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
This module contains the base definitions for xmlschema's converters.
"""
import re
from collections.abc import Callable, Container, Iterator, Mapping, MutableMapping
from typing import Any, NamedTuple, Optional, Union, TypeVar, TYPE_CHECKING, cast

from xmlschema.aliases import NsmapType, ElementType, XmlnsType, SchemaType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError
from xmlschema.utils.decoding import iter_decoded_data
from xmlschema.utils.misc import iter_class_slots, deprecated
from xmlschema.utils.qnames import get_namespace_map, update_namespaces, local_name
from xmlschema.resources import XMLResource
from xmlschema.locations import NamespaceResourcesMap
from xmlschema.arguments import NsMapperArguments

if TYPE_CHECKING:
    from xmlschema.validators import XsdComponent, XsdElement  # noqa: F401

__all__ = ('NamespaceMapper', 'NamespaceResourcesMap', 'NamespaceView')


class NamespaceMapperContext(NamedTuple):
    obj: Union[ElementType, Any]
    level: int
    xmlns: XmlnsType
    namespaces: NsmapType
    reverse: NsmapType


class NamespaceMapper(MutableMapping[str, str]):
    """
    A class to map/unmap namespace prefixes to URIs. An internal reverse mapping
    from URI to prefix is also maintained for keep name mapping consistent within
    updates.

    :param namespaces: initial data with mapping of namespace prefixes to URIs.
    :param process_namespaces: whether to use namespace information in name mapping \
    methods. If set to `False` then the name mapping methods simply return the \
    provided name.
    :param strip_namespaces: if set to `True` then the name mapping methods return \
    the local part of the provided name.
    :param xmlns_processing: defines the processing mode of XML namespace declarations. \
    The preferred mode is 'stacked', the mode that processes the namespace declarations \
    using a stack of contexts related with elements and levels. \
    This is the processing mode that always matches the XML namespace declarations \
    defined in the XML document. Provide 'collapsed' for loading all namespace \
    declarations of the XML source in a single map, renaming colliding prefixes. \
    Provide 'root-only' to use only the namespace declarations of the XML document root. \
    Provide 'none' to not use any namespace declaration of the XML document. \
    For default the xmlns processing mode is 'stacked' if the XML source is an \
    `XMLResource` instance, otherwise is 'none'.
    :param source: the origin of XML data. Con be an `XMLResource` instance, an XML \
    decoded data or `None`.
    """
    __slots__ = ('namespaces', 'process_namespaces', 'strip_namespaces',
                 'xmlns_processing', 'source', '__dict__', '_use_namespaces',
                 '_xmlns_getter', '_xmlns_contexts', '_reverse')

    _arguments = NsMapperArguments
    _xmlns_getter: Optional[Callable[[ElementType], XmlnsType]]
    _xmlns_contexts: list[NamespaceMapperContext]

    def __init__(self, namespaces: Optional[NsmapType] = None,
                 process_namespaces: bool = True,
                 strip_namespaces: bool = False,
                 xmlns_processing: Optional[str] = None,
                 source: Optional[Any] = None) -> None:

        self.process_namespaces = process_namespaces
        self.strip_namespaces = strip_namespaces
        self.source = source

        if xmlns_processing is None:
            self.xmlns_processing = self.xmlns_processing_default
        else:
            self.xmlns_processing = xmlns_processing

        if self.xmlns_processing == 'none':
            self._xmlns_getter = None
        elif isinstance(source, XMLResource):
            self._xmlns_getter = source.get_xmlns
        else:
            self._xmlns_getter = self.get_xmlns_from_data

        self._use_namespaces = bool(process_namespaces and not strip_namespaces)
        self.namespaces = self.get_namespaces(namespaces)
        self._reverse = {v: k and k + ':' for k, v in reversed(self.namespaces.items())}
        self._xmlns_contexts = []
        self._arguments.validate(self)

    def __getitem__(self, prefix: str) -> str:
        return self.namespaces[prefix]

    def __setitem__(self, prefix: str, uri: str) -> None:
        self.namespaces[prefix] = uri
        self._reverse[uri] = prefix and prefix + ':'

    def __delitem__(self, prefix: str) -> None:
        uri = self.namespaces.pop(prefix)
        del self._reverse[uri]

        for k in reversed(self.namespaces.keys()):
            if self.namespaces[k] == uri:
                self._reverse[uri] = k and k + ':'
                break

    def __iter__(self) -> Iterator[str]:
        return iter(self.namespaces)

    def __len__(self) -> int:
        return len(self.namespaces)

    @property
    def default_namespace(self) -> Optional[str]:
        return self.namespaces.get('')

    @property
    def xmlns_processing_default(self) -> str:
        return 'stacked' if isinstance(self.source, XMLResource) else 'none'

    def __copy__(self) -> 'NamespaceMapper':
        mapper: 'NamespaceMapper' = object.__new__(self.__class__)

        for attr in iter_class_slots(self):
            value = getattr(self, attr)
            if isinstance(value, (dict, list)):
                setattr(mapper, attr, value.copy())
            else:
                setattr(mapper, attr, value)

        return mapper

    def clear(self) -> None:
        self.namespaces.clear()
        self._reverse.clear()
        self._xmlns_contexts.clear()

    def get_xmlns_from_data(self, obj: Any) -> XmlnsType:
        """Returns the XML declarations from decoded element data."""
        return None

    def get_namespaces(self, namespaces: Optional[NsmapType] = None,
                       root_only: bool = True) -> dict[str, str]:
        """
        Extracts namespaces with related prefixes from the XML source. It the XML
        source is an `XMLResource` instance delegates the extraction to it.
        With XML decoded data iterates the source try to extract xmlns information
        using the implementation of *get_xmlns_from_data()*. If xmlns processing
        mode is 'none', no namespace declaration is retrieved from the XML source.
        Arguments and return type are identical to the ones defined for the method
        *get_namespaces()* of `XMLResource` class.
        """
        if self._xmlns_getter is None:
            return get_namespace_map(namespaces)
        elif isinstance(self.source, XMLResource):
            return self.source.get_namespaces(namespaces, root_only)

        xmlns: XmlnsType
        namespaces = get_namespace_map(namespaces)
        for obj, level in iter_decoded_data(self.source):
            if level <= 1:  # root xmlns declarations are usually at level 0 or 1
                if xmlns := self.get_xmlns_from_data(obj):
                    update_namespaces(namespaces, xmlns, True)
            elif root_only:
                break
            elif xmlns := self.get_xmlns_from_data(obj):
                update_namespaces(namespaces, xmlns)

        return namespaces

    @deprecated('5.0')
    def set_context(self, obj: Any, level: int) -> XmlnsType:
        return self.set_xmlns_context(obj, level)

    def set_xmlns_context(self, obj: Any, level: int) -> XmlnsType:
        """
        Set the right context for the XML data and its level, updating the namespace
        map if necessary. Returns the xmlns declarations of the provided XML data.
        """
        xmlns = None

        if self._xmlns_contexts:
            # Remove contexts of sibling or descendant elements
            namespaces = reverse = None

            while self._xmlns_contexts:  # pragma: no cover
                context = self._xmlns_contexts[-1]
                if level > context.level:
                    break
                elif level == context.level and context.obj is obj:
                    # The context for (obj, level) already exists
                    xmlns = context.xmlns
                    break

                namespaces, reverse = self._xmlns_contexts.pop()[-2:]

            if namespaces is not None and reverse is not None:
                self.namespaces.clear()
                self.namespaces.update(namespaces)
                self._reverse.clear()
                self._reverse.update(reverse)

        if xmlns or not self._xmlns_getter:
            return xmlns

        xmlns = self._xmlns_getter(obj)
        if xmlns:
            if self.xmlns_processing == 'stacked':
                context = NamespaceMapperContext(
                    obj,
                    level,
                    xmlns,
                    {k: v for k, v in self.namespaces.items()},
                    {k: v for k, v in self._reverse.items()},
                )
                self._xmlns_contexts.append(context)
                self.namespaces.update(xmlns)
                if level:
                    self._reverse.update((v, k and k + ':') for k, v in xmlns)
                else:
                    self._reverse.update((v, k and k + ':') for k, v in reversed(xmlns)
                                         if v not in self._reverse)
                return xmlns

            elif not level or self.xmlns_processing == 'collapsed':
                for prefix, uri in xmlns:
                    if not prefix:
                        if not uri:
                            continue
                        elif '' not in self.namespaces:
                            if not level:
                                self.namespaces[''] = uri
                                if uri not in self._reverse:
                                    self._reverse[uri] = ''
                                continue
                        elif self.namespaces[''] == uri:
                            continue
                        prefix = 'default'

                    while prefix in self.namespaces:
                        if self.namespaces[prefix] == uri:
                            break
                        match = re.search(r'(\d+)$', prefix)
                        if match:
                            index = int(match.group()) + 1
                            prefix = prefix[:match.span()[0]] + str(index)
                        else:
                            prefix += '0'
                    else:
                        self.namespaces[prefix] = uri
                        if uri not in self._reverse:
                            self._reverse[uri] = prefix and prefix + ':'
        return None

    def map_qname(self, qname: str) -> str:
        """
        Converts an extended QName to the prefixed format. Only registered
        namespaces are mapped.

        :param qname: a QName in extended format or a local name.
        :return: a QName in prefixed format or a local name.
        """
        if not self._use_namespaces:
            return local_name(qname) if self.strip_namespaces else qname

        try:
            if qname[0] != '{' or not self.namespaces:
                return qname
            namespace, local_part = qname[1:].split('}')
        except IndexError:
            return qname
        except ValueError:
            raise XMLSchemaValueError("the argument 'qname' has an invalid value %r" % qname)
        except TypeError:
            raise XMLSchemaTypeError("the argument 'qname' must be a string-like object")

        try:
            return self._reverse[namespace] + local_part
        except KeyError:
            return qname

    def unmap_qname(self, qname: str,
                    name_table: Optional[Container[Optional[str]]] = None,
                    xmlns: Optional[list[tuple[str, str]]] = None) -> str:
        """
        Converts a QName in prefixed format or a local name to the extended QName format.
        Local names are converted only if a default namespace is included in the instance.
        If a *name_table* is provided a local name is mapped to the default namespace
        only if not found in the name table.

        :param qname: a QName in prefixed format or a local name
        :param name_table: an optional lookup table for checking local names.
        :param xmlns: an optional list of namespace declarations that integrate \
        or override the namespace map.
        :return: a QName in extended format or a local name.
        """
        namespaces: MutableMapping[str, str]

        if not self._use_namespaces:
            return local_name(qname) if self.strip_namespaces else qname

        if xmlns:
            namespaces = {k: v for k, v in self.namespaces.items()}
            namespaces.update(xmlns)
        else:
            namespaces = self.namespaces

        try:
            if qname[0] == '{' or not namespaces:
                return qname
            elif ':' in qname:
                prefix, name = qname.split(':')
            else:
                default_namespace = namespaces.get('')
                if not default_namespace:
                    return qname
                elif name_table is None or qname not in name_table:
                    return f'{{{default_namespace}}}{qname}'
                else:
                    return qname

        except IndexError:
            return qname
        except ValueError:
            raise XMLSchemaValueError("the argument 'qname' has an invalid value %r" % qname)
        except (TypeError, AttributeError):
            raise XMLSchemaTypeError("the argument 'qname' must be a string-like object")
        else:
            try:
                uri = namespaces[prefix]
            except KeyError:
                return qname
            else:
                return f'{{{uri}}}{name}' if uri else name


CT = TypeVar('CT', bound=Union['XsdComponent', set['XsdElement']])


class NamespaceView(Mapping[str, CT]):
    """
    A mapping for filtered access to a dictionary that stores objects by FQDN.
    """
    __slots__ = '_schema', '_name', '_prefix', '_prefix_len'

    def __init__(self, schema: SchemaType, name: str) -> None:
        self._schema = schema
        self._name = name
        namespace = schema.target_namespace
        self._prefix = f'{{{namespace}}}' if namespace else ''
        self._prefix_len = len(self._prefix)

    def __getitem__(self, key: str) -> CT:
        try:
            return cast(CT, getattr(self._schema.maps, self._name)[self._prefix + key])
        except KeyError:
            raise KeyError(key) from None

    def __len__(self) -> int:
        target = getattr(self._schema.maps, self._name)
        if not self._prefix:
            return sum(1 for k in target if k[:1] != '{')
        return sum(1 for k in target if self._prefix == k[:self._prefix_len])

    def __iter__(self) -> Iterator[str]:
        if not self._prefix:
            for k in getattr(self._schema.maps, self._name):
                if k[:1] != '{':
                    yield k
        else:
            for k in getattr(self._schema.maps, self._name):
                if self._prefix == k[:self._prefix_len]:
                    yield k[self._prefix_len:]

    def __repr__(self) -> str:
        return '%s(%s)' % (self.__class__.__name__, str(self.as_dict()))

    def __contains__(self, key: object) -> bool:
        return isinstance(key, str) and \
            (self._prefix + key) in getattr(self._schema.maps, self._name)

    def __eq__(self, other: Any) -> Any:
        return self.as_dict() == other

    def as_dict(self) -> dict[str, CT]:
        target = getattr(self._schema.maps, self._name)
        if not self._prefix:
            return {k: v for k, v in target.items() if k[:1] != '{'}
        else:
            return {
                k[self._prefix_len:]: v for k, v in target.items()
                if self._prefix == k[:self._prefix_len]
            }
