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
A unified loader module for ElementTree with a safe parser and helper functions.
"""
import re
import io
import importlib
from collections import Counter
from collections.abc import Iterator, MutableMapping
from pyexpat import XMLParserType
from typing import cast, Any, Optional, Union
from xml.dom import pulldom
from xml.etree import ElementTree
from xml.sax import SAXParseException
from xml.sax import expatreader  # type: ignore[attr-defined, unused-ignore]

from elementpath.protocols import ElementProtocol, DocumentProtocol, EtreeElementProtocol, \
    LxmlElementProtocol
from elementpath.exceptions import XMLResourceForbidden, ElementPathOSError

ElementType = Union[EtreeElementProtocol, LxmlElementProtocol, ElementTree.Element]


class SafeExpatParser(expatreader.ExpatParser):  # type: ignore[misc, unused-ignore]
    _parser: XMLParserType

    def forbid_entity_declaration(self, name, is_parameter_entity,  # type: ignore
                                  value, base, sysid, pubid, notation_name):
        raise XMLResourceForbidden(f"Entities are forbidden (entity_name={name!r})")

    def forbid_unparsed_entity_declaration(self, name, base,  # type: ignore
                                           sysid, pubid, notation_name):
        raise XMLResourceForbidden(f"Unparsed entities are forbidden (entity_name={name!r})")

    def forbid_external_entity_reference(self, context, base, sysid, pubid):  # type: ignore
        raise XMLResourceForbidden(
            f"External references are forbidden (system_id={sysid!r}, public_id={pubid!r})"
        )  # pragma: no cover

    def reset(self) -> None:
        super().reset()
        self._parser.EntityDeclHandler = self.forbid_entity_declaration
        self._parser.UnparsedEntityDeclHandler = self.forbid_unparsed_entity_declaration
        self._parser.ExternalEntityRefHandler = self.forbid_external_entity_reference


def defuse_xml(xml_source: Union[str, bytes]) -> Union[str, bytes]:
    resource: Any
    if isinstance(xml_source, str):
        resource = io.StringIO(xml_source)
    else:
        resource = io.BytesIO(xml_source)

    parser = SafeExpatParser()
    try:
        for event, node in pulldom.parse(resource, parser):
            if event == pulldom.START_ELEMENT:
                break
    except SAXParseException:
        pass  # the purpose is to defuse not to check xml source syntax
    except OSError as err:
        raise ElementPathOSError(str(err))

    return xml_source


def is_etree_element(obj: Any) -> bool:
    return hasattr(obj, 'tag') and hasattr(obj, 'attrib') and hasattr(obj, 'text')


def is_lxml_etree_element(obj: Any) -> bool:
    return is_etree_element(obj) and \
        hasattr(obj, 'getparent') and \
        hasattr(obj, 'nsmap') and \
        obj.__class__.__module__ in ('lxml.etree', 'lxml.html')


def is_etree_element_instance(obj: Any) -> bool:
    """Strictly checks that the objects is an ElementTree or lxml.etree Element."""
    return isinstance(obj, ElementTree.Element) or is_lxml_etree_element(obj)


def is_etree_document(obj: Any) -> bool:
    return hasattr(obj, 'getroot') and hasattr(obj, 'parse') and hasattr(obj, 'iter')


def is_lxml_etree_document(obj: Any) -> bool:
    return is_etree_document(obj) and \
        hasattr(obj, 'xpath') and \
        hasattr(obj, 'xslt') and \
        obj.__class__.__module__ in ('lxml.etree', 'lxml.html')


def is_etree_document_instance(obj: Any) -> bool:
    """Strictly checks that the objects is an ElementTree or lxml.etree document."""
    return isinstance(obj, ElementTree.ElementTree) or is_lxml_etree_document(obj)


def etree_iter_strings(elem: Union[DocumentProtocol, ElementProtocol],
                       normalize: bool = False) -> Iterator[str]:
    e: ElementProtocol
    if normalize:
        if hasattr(elem, 'getroot'):
            root = cast(DocumentProtocol, elem).getroot()
            if root is None:
                return
        else:
            root = elem

        for e in elem.iter():
            if callable(e.tag):
                continue
            if e.text is not None:
                yield e.text.strip() if e is root else e.text
            if e.tail is not None and e is not root:
                yield e.tail.strip() if e in root else e.tail
    else:
        for e in elem.iter():
            if callable(e.tag):
                continue
            if e.text is not None:
                yield e.text
            if e.tail is not None and e is not elem:
                yield e.tail


def etree_deep_equal(e1: ElementProtocol, e2: ElementProtocol) -> bool:
    if e1.tag != e2.tag:
        return False
    elif (e1.text or '').strip() != (e2.text or '').strip():
        return False
    elif (e1.tail or '').strip() != (e2.tail or '').strip():
        return False
    elif e1.attrib != e2.attrib:
        return False
    elif len(e1) != len(e2):
        return False
    return all(etree_deep_equal(c1, c2) for c1, c2 in zip(e1, e2))


def etree_iter_paths(elem: ElementProtocol, path: str = '.') \
        -> Iterator[tuple[ElementProtocol, str]]:

    yield elem, path
    comment_nodes = 0
    pi_nodes = Counter[Optional[str]]()
    positions = Counter[Optional[str]]()

    for child in elem:
        if callable(child.tag):
            if child.tag.__name__ == 'Comment':
                comment_nodes += 1
                yield child, f'{path}/comment()[{comment_nodes}]'
                continue

            try:
                name = cast(str, child.target)  # type: ignore[attr-defined]
            except AttributeError:
                assert child.text is not None
                name = child.text.split(' ', maxsplit=1)[0]

            pi_nodes[name] += 1
            yield child, f'{path}/processing-instruction({name})[{pi_nodes[name]}]'
            continue

        if child.tag.startswith('{'):
            tag = f'Q{child.tag}'
        else:
            tag = f'Q{{}}{child.tag}'

        if path == '/':
            child_path = f'/{tag}'
        elif path:
            child_path = '/'.join((path, tag))
        else:
            child_path = tag

        positions[child.tag] += 1
        child_path += f'[{positions[child.tag]}]'

        yield from etree_iter_paths(child, child_path)


def etree_tostring(elem: ElementType,
                   namespaces: Optional[MutableMapping[str, str]] = None,
                   indent: str = '',
                   max_lines: Optional[int] = None,
                   spaces_for_tab: Optional[int] = 4,
                   xml_declaration: Optional[bool] = None,
                   encoding: str = 'unicode',
                   method: str = 'xml') -> Union[str, bytes]:
    """
    Serialize an Element tree to a string.

    :param elem: the Element instance.
    :param namespaces: is an optional mapping from namespace prefix to URI. \
    Provided namespaces are registered before serialization. Ignored if the \
    provided *elem* argument is a lxml Element instance.
    :param indent: the baseline indentation.
    :param max_lines: if truncate serialization after a number of lines \
    (default: do not truncate).
    :param spaces_for_tab: number of spaces for replacing tab characters. For \
    default tabs are replaced with 4 spaces, provide `None` to keep tab characters.
    :param xml_declaration: if set to `True` inserts the XML declaration at the head.
    :param encoding: if "unicode" (the default) the output is a string, \
    otherwise it’s binary.
    :param method: is either "xml" (the default), "html" or "text".
    :return: a Unicode string.
    """
    def reindent(line: str) -> str:
        if not line:
            return line
        elif line.startswith(min_indent):
            return line[start:] if start >= 0 else indent[start:] + line
        else:
            return indent + line

    etree_module: Any
    if not is_etree_element_instance(elem):
        raise TypeError(f"{elem!r} is not an Element")
    elif not hasattr(elem, 'nsmap'):
        etree_module = ElementTree
    else:
        etree_module = importlib.import_module('lxml.etree')

    if namespaces and not hasattr(elem, 'nsmap'):
        default_namespace = namespaces.get('')
        for prefix, uri in namespaces.items():
            if prefix and not re.match(r'ns\d+$', prefix):
                etree_module.register_namespace(prefix, uri)
                if uri == default_namespace:
                    default_namespace = None

        if default_namespace:
            etree_module.register_namespace('', default_namespace)

    xml_text = etree_module.tostring(elem, encoding=encoding, method=method)
    if isinstance(xml_text, bytes):
        xml_text = xml_text.decode('utf-8')

    if spaces_for_tab is not None:
        xml_text = xml_text.replace('\t', ' ' * spaces_for_tab)

    if xml_text.startswith('<?xml '):
        if xml_declaration is False:
            lines = xml_text.splitlines()[1:]
        else:
            lines = xml_text.splitlines()
    elif xml_declaration and encoding.lower() != 'unicode':
        lines = ['<?xml version="1.0" encoding="{}"?>'.format(encoding)]
        lines.extend(xml_text.splitlines())
    else:
        lines = xml_text.splitlines()

    # Clear ending empty lines
    while lines and not lines[-1].strip():
        lines.pop(-1)

    if not lines or method == 'text' or (not indent and not max_lines):
        if encoding == 'unicode':
            return '\n'.join(lines)
        return '\n'.join(lines).encode(encoding)

    last_indent = ' ' * min(k for k in range(len(lines[-1])) if lines[-1][k] != ' ')
    if len(lines) > 2:
        try:
            child_indent = ' ' * min(
                k for line in lines[1:-1] for k in range(len(line)) if line[k] != ' '
            )
        except ValueError:
            child_indent = ''

        min_indent = min(child_indent, last_indent)
    else:
        min_indent = child_indent = last_indent

    start = len(min_indent) - len(indent)

    if max_lines is not None and len(lines) > max_lines + 2:
        lines = lines[:max_lines] + [child_indent + '...'] * 2 + lines[-1:]

    if encoding == 'unicode':
        return '\n'.join(reindent(line) for line in lines)
    return '\n'.join(reindent(line) for line in lines).encode(encoding)


__all__ = ['SafeExpatParser', 'defuse_xml', 'is_etree_element', 'is_lxml_etree_element',
           'is_etree_element_instance', 'is_etree_document', 'is_lxml_etree_document',
           'is_etree_document_instance', 'etree_iter_strings', 'etree_deep_equal',
           'etree_iter_paths', 'etree_tostring']
