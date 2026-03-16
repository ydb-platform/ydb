#
# Copyright (c), 2022-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import json
from collections.abc import Iterator, Iterable
from decimal import Decimal, ROUND_UP
from types import ModuleType
from typing import cast, Any, Optional, Union
from xml.etree import ElementTree

from elementpath.exceptions import ElementPathError, xpath_error
from elementpath.namespaces import XSLT_XQUERY_SERIALIZATION_NAMESPACE
from elementpath.datatypes import AnyAtomicType, AnyURI, AbstractDateTime, \
    AbstractBinary, UntypedAtomic, QName
from elementpath.xpath_nodes import XPathNode, ElementNode, AttributeNode, DocumentNode, \
    NamespaceNode, TextNode, CommentNode
from elementpath.xpath_nodes import EtreeElementNode
from elementpath.xpath_tokens import XPathToken, XPathMap, XPathArray
from elementpath.protocols import EtreeElementProtocol, LxmlElementProtocol

# XSLT and XQuery Serialization parameters
SERIALIZATION_PARAMS = '{%s}serialization-parameters' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_OMIT_XML_DECLARATION = '{%s}omit-xml-declaration' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_USE_CHARACTER_MAPS = '{%s}use-character-maps' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_CHARACTER_MAP = '{%s}character-map' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_METHOD = '{%s}method' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_INDENT = '{%s}indent' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_VERSION = '{%s}version' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_CDATA = '{%s}cdata-section-elements' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_NO_INDENT = '{%s}suppress-indentation' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_STANDALONE = '{%s}standalone' % XSLT_XQUERY_SERIALIZATION_NAMESPACE
SER_PARAM_ITEM_SEPARATOR = '{%s}item-separator' % XSLT_XQUERY_SERIALIZATION_NAMESPACE


def get_serialization_params(params: Union[None, ElementNode, XPathMap] = None,
                             token: Optional[XPathToken] = None) -> dict['str', Any]:

    kwargs: dict[str, Any] = {}
    character_map: dict[str, str]
    value: Any

    if isinstance(params, XPathMap):
        if len(params[:]) > len(params.keys()):  # pragma: no cover
            raise xpath_error('SEPM0019', token=token)

        for key, value in params.items():
            if not isinstance(key, str) or value is None:
                continue
            elif isinstance(value, UntypedAtomic):
                value = str(value)
                if value == 'true':
                    value = True
                elif value == 'false':
                    value = False

            if key == 'omit-xml-declaration':
                if not isinstance(value, bool):
                    raise xpath_error('XPTY0004', token=token)
                kwargs['xml_declaration'] = not value

            elif key == 'cdata-section-elements':
                # TODO: doesn't work within element nodes
                if isinstance(value, XPathArray):
                    value = value.items()
                if not isinstance(value, list) or \
                        not all(isinstance(x, QName) for x in value):
                    raise xpath_error('XPTY0004', token=token)
                kwargs['cdata_section'] = value

            elif key == 'method':
                if value not in ('html', 'xml', 'xhtml', 'text', 'adaptive', 'json'):
                    raise xpath_error('SEPM0017', token=token)
                kwargs[key] = value if value != 'xhtml' else 'html'

            elif key == 'indent':
                if not isinstance(value, bool):
                    raise xpath_error('XPTY0004', token=token)
                kwargs[key] = value

            elif key == 'item-separator':
                if not isinstance(value, str):
                    raise xpath_error('XPTY0004', token=token)
                kwargs['item_separator'] = value

            elif key == 'use-character-maps':
                if not isinstance(value, XPathMap):
                    raise xpath_error('XPTY0004', token=token)

                kwargs['character_map'] = character_map = {}
                for k, v in value.items():
                    if not isinstance(k, str) or not isinstance(v, str):
                        raise xpath_error('XPTY0004', token=token)
                    elif len(k) != 1:
                        msg = f'invalid character {k!r} in character map'
                        raise xpath_error('SEPM0016', msg, token)
                    else:
                        character_map[k] = v

            elif key == 'suppress-indentation':  # pragma: no cover
                if isinstance(value, QName) or isinstance(value, list) \
                        and all(isinstance(x, QName) for x in value):
                    kwargs[key] = value
                else:
                    raise xpath_error('XPTY0004', token=token)
            elif key == 'standalone':
                if not value and isinstance(value, list):
                    pass
                elif isinstance(value, bool):
                    kwargs['standalone'] = value
                else:
                    if value not in ('yes', 'no', 'omit'):
                        raise xpath_error('XPTY0004', token=token)
                    if value != 'omit':
                        kwargs['standalone'] = value == 'yes'

            elif key == 'json-node-output-method':
                if not isinstance(value, (str, QName)):
                    raise xpath_error('XPTY0004', token=token)
                kwargs[key] = value

            elif key == 'allow-duplicate-names':
                if value is not None and not isinstance(value, bool):
                    raise xpath_error('XPTY0004', token=token)
                kwargs['allow_duplicate_names'] = value

            elif key == 'encoding':
                if not isinstance(value, str):
                    raise xpath_error('XPTY0004', token=token)
                kwargs[key] = value

            elif key == 'html-version':
                if not isinstance(value, (int, Decimal)):
                    raise xpath_error('XPTY0004', token=token)
                kwargs[key] = value

    elif isinstance(params, ElementNode):
        root = cast(Union[EtreeElementProtocol, LxmlElementProtocol], params.value)
        if root.tag != SERIALIZATION_PARAMS:
            msg = 'output:serialization-parameters tag expected'
            raise xpath_error('XPTY0004', msg, token)

        if len(root) > len({e.tag for e in root}):
            raise xpath_error('SEPM0019', token=token)

        for child in root:
            if callable(child.tag):
                continue
            elif child.tag == SER_PARAM_OMIT_XML_DECLARATION:
                value = child.get('value')
                if value not in ('yes', 'no') or len(child.attrib) > 1:
                    raise xpath_error('SEPM0017', token=token)
                elif value == 'no':
                    kwargs['xml_declaration'] = True

            elif child.tag == SER_PARAM_USE_CHARACTER_MAPS:
                if len(child.attrib):
                    raise xpath_error('SEPM0017', token=token)

                kwargs['character_map'] = character_map = {}
                for e in child:
                    if e.tag != SER_PARAM_CHARACTER_MAP:
                        raise xpath_error('SEPM0017', token=token)

                    try:
                        character = e.attrib['character']
                        if character in character_map:
                            msg = 'duplicate character {!r} in character map'
                            raise xpath_error('SEPM0018', msg.format(character), token)
                        elif len(character) != 1:
                            msg = 'invalid character {!r} in character map'
                            raise xpath_error('SEPM0017', msg.format(character), token)

                        character_map[character] = e.attrib['map-string']
                    except KeyError as key:
                        msg = "missing {} in character map"
                        raise xpath_error('SEPM0017', msg.format(key)) from None
                    else:
                        if len(e.attrib) > 2:
                            msg = "invalid attribute in character map"
                            raise xpath_error('SEPM0017', msg)

            elif child.tag == SER_PARAM_METHOD:
                value = child.get('value')
                if value not in ('html', 'xml', 'xhtml', 'text') or len(child.attrib) > 1:
                    raise xpath_error('SEPM0017', token=token)
                kwargs['method'] = value if value != 'xhtml' else 'html'

            elif child.tag == SER_PARAM_INDENT:
                value = child.attrib.get('value', '')
                assert isinstance(value, str)

                value = value.strip()
                if value not in ('yes', 'no') or len(child.attrib) > 1:
                    raise xpath_error('SEPM0017', token=token)

            elif child.tag == SER_PARAM_ITEM_SEPARATOR:
                try:
                    kwargs['item_separator'] = child.attrib['value']
                except KeyError:
                    raise xpath_error('SEPM0017', token=token) from None

            elif child.tag == SER_PARAM_CDATA:
                pass  # TODO param
            elif child.tag == SER_PARAM_NO_INDENT:
                pass  # TODO param
            elif child.tag == SER_PARAM_STANDALONE:
                value = child.attrib.get('value', '')
                assert isinstance(value, str)

                value = value.strip()
                if value not in ('yes', 'no', 'omit') or len(child.attrib) > 1:
                    raise xpath_error('SEPM0017', token=token)
                if value != 'omit':
                    kwargs['standalone'] = value == 'yes'

            elif child.tag.startswith(f'{{{XSLT_XQUERY_SERIALIZATION_NAMESPACE}'):
                raise xpath_error('SEPM0017', token=token)
            elif not child.tag.startswith('{'):  # no-namespace not allowed
                raise xpath_error('SEPM0017', token=token)

    return kwargs


def iter_normalized(elements: Iterable[Any],
                    item_separator: Optional[str] = None) -> Iterator[Any]:
    chunks = []
    sep = ' ' if item_separator is None else item_separator

    for item in elements:
        if isinstance(item, XPathArray):
            for _item in item.iter_flatten():
                if isinstance(_item, bool):
                    chunks.append('true' if _item else 'false')
                elif isinstance(_item, AnyAtomicType):
                    chunks.append(str(_item))
                else:
                    if chunks:
                        yield sep.join(chunks)
                        chunks.clear()
                    if isinstance(_item, DocumentNode):
                        yield from _item.children
                    else:
                        yield _item

        elif isinstance(item, bool):
            chunks.append('true' if item else 'false')
        elif isinstance(item, AnyAtomicType):
            chunks.append(str(item))
        else:
            if chunks:
                yield sep.join(chunks)
                chunks.clear()
            if isinstance(item, DocumentNode):
                yield from item.children
            else:
                yield item
    else:
        if chunks:
            yield sep.join(chunks)


def serialize_to_xml(elements: Iterable[Any],
                     etree_module: Optional[ModuleType] = None,
                     token: Optional['XPathToken'] = None,
                     **params: Any) -> str:
    if etree_module is None:
        etree_module = ElementTree

    item_separator = params.get('item_separator')
    character_map = params.get('character_map')

    cdata_section: Union[set[str], tuple[()]]
    kwargs = {}
    if 'xml_declaration' in params:
        kwargs['xml_declaration'] = params['xml_declaration']
    if 'standalone' in params:
        kwargs['standalone'] = params['standalone']
    if 'cdata_section' in params:
        cdata_section = {x.expanded_name for x in params['cdata_section']}
    else:
        cdata_section = ()

    method = kwargs.get('method', 'xml')
    if method == 'xhtml':
        method = 'html'

    chunks = []
    for item in iter_normalized(elements, item_separator):
        if isinstance(item, ElementNode):
            assert isinstance(item, EtreeElementNode)
            elem = item.value
        elif isinstance(item, (AttributeNode, NamespaceNode)):
            raise xpath_error('SENR0001', token=token)
        elif isinstance(item, TextNode):
            if item.parent is not None and item.parent.name in cdata_section:
                chunks.append(f'<![CDATA[{item.value}]]>')
            else:
                chunks.append(item.value)
            continue
        elif not isinstance(item, str):
            raise xpath_error('SENR0001', token=token)
        else:
            chunks.append(item)
            continue

        try:
            cks = etree_module.tostringlist(
                elem, encoding='utf-8', method=method, **kwargs
            )
        except TypeError:
            ck = etree_module.tostring(elem, encoding='utf-8', method=method)
            chunks.append(ck.decode('utf-8').rstrip(elem.tail))
        else:
            if cks and cks[0].startswith(b'<?'):
                cks[0] = cks[0].replace(b'\'', b'"')
            chunks.append(b'\n'.join(cks).decode('utf-8').rstrip(elem.tail))

    if not character_map:
        return (item_separator or '').join(chunks)

    result = (item_separator or '').join(chunks)
    for character, map_string in character_map.items():
        result = result.replace(character, map_string)
    return result


def serialize_to_json(elements: Iterable[Any],
                      etree_module: Optional[ModuleType] = None,
                      token: Optional['XPathToken'] = None,
                      **params: Any) -> str:
    if etree_module is None:
        etree_module = ElementTree

    class MapEncodingDict(dict):  # type: ignore[type-arg]
        def __init__(self, items: Any) -> None:
            self[None] = None
            self._items = items

        def items(self) -> Any:
            return self._items

    class XPathEncoder(json.JSONEncoder):

        def default(self, obj: Any) -> Any:
            if isinstance(obj, XPathNode):
                if isinstance(obj, DocumentNode):
                    return ''.join(self.default(child) for child in obj)
                elif isinstance(obj, ElementNode):
                    assert isinstance(obj, EtreeElementNode)
                    elem = obj.value
                    assert etree_module is not None

                    try:
                        chunks = etree_module.tostringlist(elem, encoding='utf-8')
                    except TypeError:
                        chunk = etree_module.tostring(elem, encoding='utf-8')
                        return cast(str, chunk.decode('utf-8'))
                    else:
                        if chunks and chunks[0].startswith(b'<?'):
                            chunks[0] = chunks[0].replace(b'\'', b'"')
                        return b'\n'.join(chunks).decode('utf-8')

                elif isinstance(obj, (AttributeNode, NamespaceNode)):
                    return f'{obj.name}="{obj.string_value}"'
                elif isinstance(obj, TextNode):
                    return obj.value
                elif isinstance(obj, CommentNode):
                    return f'<!--{obj.string_value}-->'
                else:
                    return f'<?{obj.name} {obj.string_value}?>'
            elif isinstance(obj, XPathMap):
                map_keys = set()
                map_items = []
                k: Any
                for k, v in obj.items():
                    if isinstance(k, QName):
                        k = str(k)
                    map_items.append((k, v))

                    if k not in map_keys:
                        map_keys.add(k)
                    elif not params.get('allow_duplicate_names'):
                        raise xpath_error('SERE0022', token=token)
                return MapEncodingDict(map_items)

            elif isinstance(obj, XPathArray):
                return [v if v or not isinstance(v, list) else None for v in obj.items()]
            elif isinstance(obj, (AbstractBinary, AbstractDateTime, AnyURI, UntypedAtomic)):
                return str(obj)
            elif isinstance(obj, Decimal):
                return float(Decimal(obj).quantize(Decimal("0.01"), ROUND_UP))
            elif isinstance(obj, list):
                return [v for v in obj]
            else:
                return super().default(obj)

    kwargs: dict[str, Any] = {
        'cls': XPathEncoder,
        'ensure_ascii': True,
        'separators': (',', ':'),
        'allow_nan': False,
    }
    try:
        parts = [json.dumps(x, **kwargs) for x in elements]
    except ElementPathError:
        raise
    except ValueError:
        raise xpath_error('SERE0020', token=token)
    except TypeError:
        raise xpath_error('SERE0021', token=token)

    if not parts:
        return 'null'
    elif len(parts) > 1:
        raise xpath_error('SERE0023', token=token)

    result = parts[0].replace('/', '\\/')

    if 'encoding' in params:
        return result.encode('utf-8').decode(params['encoding'])
    return result
