# Copyright 2014 Tomo Krajina
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import inspect as mod_inspect
import datetime as mod_datetime
import re as mod_re
import copy as mod_copy

from . import utils as mod_utils

from typing import *


class GPXFieldTypeConverter:
    def __init__(self, from_string: str, to_string: str) -> None:
        self.from_string = from_string
        self.to_string = to_string


RE_TIMESTAMP = mod_re.compile(
    r'^([0-9]{4})-([0-9]{1,2})-([0-9]{1,2})[T ]([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})'
    r'(\.[0-9]{1,15})?(Z|[+-−][0-9]{2}:?(?:[0-9]{2})?)?$')


class SimpleTZ(mod_datetime.tzinfo):
    __slots__ = ('offset',)

    def __init__(self, s: str="") -> None:
        self.offset = 0
        if s and len(s) >= 2:
            if s[0] in ('−', '-'):
                mult = -1
                s = s[1:]
            else:
                if s[0] == '+':
                    s = s[1:]
                mult = 1
            hour = int(s[:2]) if s[:2].isdigit() else 0
            if len(s) >= 4:
                minute = int(s[-2:]) if s[-2:].isdigit() else 0
            else:
                minute = 0
            self.offset = mult * (hour * 60 + minute)

    def utcoffset(self, dt: Optional[mod_datetime.datetime]) -> mod_datetime.timedelta:
        return mod_datetime.timedelta(minutes=self.offset)

    def dst(self, dt: Optional[mod_datetime.datetime]) -> mod_datetime.timedelta:
        return mod_datetime.timedelta(0)

    def tzname(self, dt: Optional[mod_datetime.datetime]) -> str:
        if self.offset == 0:
            return 'Z'
        return f'{self.offset // 60:02}:{self.offset % 60:02}'

    def __copy__(self) -> mod_datetime.tzinfo:
        return self.__deepcopy__()

    def __deepcopy__(self, memodict: Optional[Dict[Any, Any]]={}) -> mod_datetime.tzinfo:
        return self.__class__(self.tzname(None))

    def __repr__(self) -> str:
        return f'SimpleTZ({self.tzname(None)!r})'

    def __eq__(self, other: Any) -> bool:
        return self.offset == other.offset # type: ignore


def parse_time(string: str) -> Optional[mod_datetime.datetime]:
    from . import gpx as mod_gpx
    if not string:
        return None
    m = RE_TIMESTAMP.match(string)
    if m:
        dt = [int(m.group(i)) for i in range(1, 7)]
        if m.group(7):
            f = m.group(7)[1:7]
            dt.append(int(f + "0" * (6 - len(f))))
        else:
            dt.append(0)
        if m.group(8):
            dt.append(SimpleTZ(m.group(8))) # type: ignore
        return mod_datetime.datetime(*dt) # type: ignore
    raise mod_gpx.GPXException(f'Invalid time: {string}')


def format_time(time: mod_datetime.datetime) -> str:
    return time.isoformat().replace('+00:00', 'Z')



# ----------------------------------------------------------------------------------------------------
# Type converters used to convert from/to the string in the XML:
# ----------------------------------------------------------------------------------------------------


class FloatConverter:
    def __init__(self) -> None:
        self.from_string = lambda string : None if string is None else float(string.strip())
        self.to_string =   lambda flt    : mod_utils.make_str(flt)


class IntConverter:
    def __init__(self) -> None:
        self.from_string = lambda string: None if string is None else int(string.strip())
        self.to_string = lambda flt: str(flt)


class TimeConverter:
    def from_string(self, string: str) -> Optional[mod_datetime.datetime]:
        try:
            return parse_time(string)
        except:
            return None

    def to_string(self, time: Optional[mod_datetime.datetime]) -> Optional[str]:
        return format_time(time) if time else None


INT_TYPE = IntConverter()
FLOAT_TYPE = FloatConverter()
TIME_TYPE = TimeConverter()


# ----------------------------------------------------------------------------------------------------
# Field converters:
# ----------------------------------------------------------------------------------------------------


class AbstractGPXField:
    def __init__(self, attribute_field: Optional[str] = None, is_list: Optional[bool]=None) -> None:
        self.attribute_field = attribute_field
        self.is_list = is_list
        self.attribute: Optional[str] = None

    def from_xml(self, node: str, version: str) -> Any:
        raise Exception('Not implemented')

    def to_xml(self, value: Any, version: str, nsmap: Any) -> Optional[str]:
        raise Exception('Not implemented')


class GPXField(AbstractGPXField):
    """
    Used for to (de)serialize fields with simple field<->xml_tag mapping.
    """
    def __init__(self, name: Optional[str], tag: Optional[str]=None, attribute: Optional[str]=None, type: Any=None,
                 possible: Optional[Iterable[str]]=None, mandatory: Optional[bool]=None) -> None:
        AbstractGPXField.__init__(self)
        self.name = name
        if tag and attribute:
            from . import gpx as mod_gpx
            raise mod_gpx.GPXException('Only tag *or* attribute may be given!')

        self.tag: Optional[str] = None
        if attribute:
            self.tag = None
            self.attribute = attribute
        elif tag:
            self.tag = name if (tag is True) else tag # type: ignore
            self.attribute = None
        else:
            self.tag = name
            self.attribute = None
        self.type_converter = type
        self.possible = possible
        self.mandatory = mandatory

    def from_xml(self, node: Any, version: str) -> Any:
        if self.attribute:
            if node is not None:
                result = node.get(self.attribute)
        else:
            __node = node.find(self.tag)
            if __node is not None:
                result = __node.text
            else:
                result = None
        if result is None:
            if self.mandatory:
                from . import gpx as mod_gpx
                raise mod_gpx.GPXException(f'{self.name} is mandatory in {self.tag} (got {result})')
            return None

        if self.type_converter:
            try:
                result = self.type_converter.from_string(result)
            except Exception as e:
                from . import gpx as mod_gpx
                raise mod_gpx.GPXException(f'Invalid value for <{self.tag}>... {result} ({e})')

        if self.possible:
            if not (result in self.possible):
                from . import gpx as mod_gpx
                raise mod_gpx.GPXException(f'Invalid value "{result}", possible: {self.possible}')

        return result

    def to_xml(self, value: Any, version: str, nsmap: Any=None, prettyprint: bool=True, indent: str='') -> Optional[str]:
        if value is None:
            return ''
        if not prettyprint:
            indent = ''
        if self.attribute:
            return f'{self.attribute}="{mod_utils.make_str(value)}"'
        elif self.type_converter:
            value = self.type_converter.to_string(value)
        if self.tag:
            return mod_utils.to_xml(self.tag, content=value, escape=True,
                                    prettyprint=prettyprint, indent=indent)
        return ''


class GPXComplexField(AbstractGPXField):
    def __init__(self, name: str, classs: Any, tag: Optional[str]=None, is_list: bool=False, empty_body: bool=False) -> None:
        AbstractGPXField.__init__(self, is_list=is_list)
        self.name = name
        self.tag = tag or name
        self.classs = classs
        self.empty_body = empty_body

    def from_xml(self, node: Any, version: str) -> Any:
        if self.is_list:
            result = []
            for child in node:
                if child.tag == self.tag:
                    result.append(gpx_fields_from_xml(self.classs, child,
                                                      version))
            return result
        else:
            field_node = node.find(self.tag)
            if field_node is None:
                return None
            return gpx_fields_from_xml(self.classs, field_node, version)

    def to_xml(self, value: Any, version: str, nsmap: Dict[str, str]={}, prettyprint: bool=True, indent: str='') -> str:
        if not prettyprint:
            indent = ''
        if self.is_list:
            result = []
            for obj in value:
                result.append(gpx_fields_to_xml(obj, self.tag, version,
                                                nsmap=nsmap,
                                                prettyprint=prettyprint,
                                                indent=indent))
            return ''.join(result)
        else:
            return gpx_fields_to_xml(value, self.tag, version, prettyprint=prettyprint, indent=indent, empty_body=self.empty_body)


class GPXEmailField(AbstractGPXField):
    """
    Converts GPX1.1 email tag group from/to string.
    """
    def __init__(self, name: str, tag: Optional[str]=None):
        AbstractGPXField.__init__(self, is_list=False)
        self.name = name
        self.tag = tag or name

    def from_xml(self, node: Any, version: str) -> Any:
        """
        Extract email address.

        Args:
            node: ETree node with child node containing self.tag
            version: str of the gpx output version "1.0" or "1.1"

        Returns:
            A string containing the email address.
        """
        email_node = node.find(self.tag)
        if email_node is None:
            return ''

        email_id = email_node.get('id')
        email_domain = email_node.get('domain')
        return f'{email_id}@{email_domain}'

    def to_xml(self, value: Any, version: str, nsmap: Optional[Dict[str, str]]=None, prettyprint: bool=True, indent: str='') -> str:
        """
        Write email address to XML

        Args:
            value: str representing an email address
            version: str of the gpx output version "1.0" or "1.1"

        Returns:
            None if value is empty or str of XML representation of the
            address. Representation starts with a \n.
        """
        if not value:
            return ''

        if not prettyprint:
            indent = ''

        if '@' in value:
            pos = value.find('@')
            email_id = value[:pos]
            email_domain = value[pos+1:]
        else:
            email_id = value
            email_domain = 'unknown'

        return f'\n{indent}<{self.tag} id="{email_id}" domain="{email_domain}" />'


class GPXExtensionsField(AbstractGPXField):
    """
    GPX1.1 extensions <extensions>...</extensions> key-value type.
    """
    def __init__(self, name: str, tag: Optional[str]=None, is_list: bool=True) -> None:
        AbstractGPXField.__init__(self, is_list=is_list)
        self.name = name
        self.tag = tag or 'extensions'

    def from_xml(self, node: Any, version: str) -> Any:
        """
        Build a list of extension Elements.

        Args:
            node: Element at the root of the extensions
            version: unused, only 1.1 supports extensions

        Returns:
            a list of Element objects
        """
        result: Any = []
        extensions_node = node.find(self.tag)
        if extensions_node is None:
            return result
        for child in extensions_node:
            result.append(mod_copy.deepcopy(child))
        return result

    def _resolve_prefix(self, qname: str, nsmap: Dict[str, str]) -> str:
        """
        Convert a tag from Clark notation into prefix notation.

        Convert a tag from Clark notation using the nsmap into a
        prefixed tag. If the tag isn't in Clark notation, return the
        qname back. Converts {namespace}tag -> prefix:tag
        
        Args:
            qname: string with the fully qualified name in Clark notation
            nsmap: a dict of prefix, namespace pairs

        Returns:
            string of the tag ready to be serialized.
        """
        if nsmap is not None and '}' in qname:
            uri, _, localname = qname.partition("}")
            uri = uri.lstrip("{")
            qname = uri + ':' + localname
            for prefix, namespace in nsmap.items():
                if uri == namespace:
                    qname = prefix + ':' + localname
                    break
        return qname

    def _ETree_to_xml(self, node: Any, nsmap: Dict[str, str]={}, prettyprint: bool=True, indent: str='') -> str:
        """
        Serialize ETree element and all subelements.

        Creates a string of the ETree and all children. The prefixes are
        resolved through the nsmap for easier to read XML.

        Args:
            node: ETree with the extension data
            version: string of GPX version, must be 1.1
            nsmap: dict of prefixes and URIs
            prettyprint: boolean, when true, indent line
            indent: string prepended to tag, usually 2 spaces per level

        Returns:
            string with all the prefixed tags and data for the node
            and its children as XML.

        """
        if not prettyprint:
            indent = ''

        # Build element tag and text
        result = []
        prefixedname = self._resolve_prefix(node.tag, nsmap)
        result.append(f'\n{indent}<{prefixedname}')
        for attrib, value in node.attrib.items():
            attrib = self._resolve_prefix(attrib, nsmap)
            result.append(f' {attrib}="{value}"')
        result.append('>')
        if node.text is not None:
             result.append(node.text.strip())


        # Build subelement nodes
        for child in node:
            result.append(self._ETree_to_xml(child, nsmap,
                                             prettyprint=prettyprint,
                                             indent=f'{indent}  '))

        # Add tail and close tag
        tail = node.tail
        if tail is not None:
            tail = tail.strip()
        else:
            tail = ''
        if len(node) > 0:
            result.append(f'\n{indent}')
        result.append(f'</{prefixedname}>{tail}')

        return ''.join(result)

    def to_xml(self, value: Any, version: str, nsmap: Dict[str, str]={}, prettyprint: bool=True, indent: str='') -> str:
        """
        Serialize list of ETree.

        Creates a string of all the ETrees in the list. The prefixes are
        resolved through the nsmap for easier to read XML.

        Args:
            value: list of ETrees with the extension data
            version: string of GPX version, must be 1.1
            nsmap: dict of prefixes and URIs
            prettyprint: boolean, when true, indent line
            indent: string prepended to tag, usually 2 spaces per level

        Returns:
            string with all the prefixed tags and data for each node
            as XML.

        """
        if not prettyprint:
            indent = ''
        if not value or version != "1.1":
            return ''
        result = [f'\n{indent}<{self.tag}>']
        for extension in value:
            result.append(self._ETree_to_xml(extension, nsmap,
                                             prettyprint=prettyprint,
                                             indent=f'{indent}  '))
        result.append(f'\n{indent}</{self.tag}>')
        return ''.join(result)

# ----------------------------------------------------------------------------------------------------
# Utility methods:
# ----------------------------------------------------------------------------------------------------

def _check_dependents(gpx_object: Any, fieldname: str) -> Tuple[str, str]:
    """
    Check for data in subelements.

    Fieldname takes the form of 'tag:dep1:dep2:dep3' for an arbitrary
    number of dependents. If all the gpx_object.dep attributes are
    empty, return a sentinel value to suppress serialization of all
    subelements.

    Args:
        gpx_object: GPXField object to check for data
        fieldname: string with tag and dependents delimited with ':'

    Returns:
        Two strings. The first is a sentinel value, '/' + tag, if all
        the subelements are empty and an empty string otherwise. The
        second is the bare tag name.
    """
    if ':' in fieldname:
        children = fieldname.split(':')
        field = children.pop(0)
        for child in children:
            if getattr(gpx_object, child.lstrip('@')):
                return '', field # Child has data
        return f'/{field}', field # No child has data
    return '', fieldname # No children

def gpx_fields_to_xml(instance: Any, tag: str, version: str, custom_attributes: Dict[str, str]={},
                      nsmap: Dict[str, str]={}, prettyprint: bool=True, indent: str='', empty_body: bool=False) -> str:
    if not prettyprint:
        indent = ''
    fields = instance.gpx_10_fields
    if version == '1.1':
        fields = instance.gpx_11_fields

    tag_open = bool(tag)
    body = []
    if tag:
        body.append(f'\n{indent}<{tag}')
        if tag == 'gpx':  # write nsmap in root node
            body.append(f' xmlns="{nsmap["defaultns"]}"')
            namespaces = set(nsmap.keys())
            namespaces.remove('defaultns')
            for prefix in sorted(namespaces):
                body.append(f' xmlns:{prefix}="{nsmap[prefix]}"')
        if custom_attributes:
            # Make sure to_xml() always return attributes in the same order:
            for key in sorted(custom_attributes.keys()):
                body.append(f' {key}="{mod_utils.make_str(custom_attributes[key])}"')
    suppressuntil = ''
    for gpx_field in fields:
        # strings indicate non-data container tags with subelements
        if isinstance(gpx_field, str):
            # Suppress empty tags
            if suppressuntil:
                if suppressuntil == gpx_field:
                    suppressuntil = ''
            else:
                suppressuntil, gpx_field = _check_dependents(instance,
                                                             gpx_field)
                if not suppressuntil:
                    if tag_open:
                        body.append('>')
                        tag_open = False
                    if gpx_field[0] == '/':
                        body.append(f'\n{indent}<{gpx_field}>')
                        if prettyprint and len(indent) > 1:
                            indent = indent[:-2]
                    else:
                        if prettyprint:
                            indent += '  '
                        body.append(f'\n{indent}<{gpx_field}')
                        tag_open = True
        elif not suppressuntil:
            value = getattr(instance, gpx_field.name)
            if gpx_field.attribute:
                body.append(' ' + gpx_field.to_xml(value, version, nsmap,
                                                   prettyprint=prettyprint,
                                                   indent=f'{indent}  '))
            elif value is not None:
                if tag_open:
                    body.append('>')
                    tag_open = False
                xml_value = gpx_field.to_xml(value, version, nsmap,
                                             prettyprint=prettyprint,
                                             indent=f'{indent}  ')
                if xml_value:
                    body.append(xml_value)

    if tag:
        if empty_body:
            body.append(' />')
            #print("tag,body:",tag,'\t',body)
        else:
            if tag_open:
                body.append('>')
            body.append('\n' + indent + '</' + tag + '>')
    return ''.join(body)


def gpx_fields_from_xml(class_or_instance: Any, node: str, version: str) -> Any:
    if mod_inspect.isclass(class_or_instance):
        result = class_or_instance()
    else:
        result = class_or_instance

    fields = result.gpx_10_fields
    if version == '1.1':
        fields = result.gpx_11_fields

    node_path: List[Union[str, int]] = [node]

    for gpx_field in fields:
        current_node = node_path[-1]
        if isinstance(gpx_field, str):
            gpx_field = gpx_field.partition(':')[0]
            if gpx_field.startswith('/'):
                node_path.pop()
            else:
                if current_node is None:
                    node_path.append(None)
                else:
                    node_path.append(cast(str, current_node).find(gpx_field))
        else:
            if current_node is not None:
                value = gpx_field.from_xml(current_node, version)
                setattr(result, gpx_field.name, value)
            elif gpx_field.attribute:
                value = gpx_field.from_xml(node, version)
                setattr(result, gpx_field.name, value)

    return result

def gpx_check_slots_and_default_values(classs: Callable[[], Any]) -> None:
    """
    Will fill the default values for this class. Instances will inherit those
    values so we don't need to fill default values for every instance.
    """
    fields = classs.gpx_10_fields + classs.gpx_11_fields # type: ignore

    instance = classs()

    try:
        attributes = [x for x in dir(instance)
                      if not x.startswith(('_', 'gpx_'))
                      and not callable(getattr(instance, x))]
    except Exception as e:
        raise Exception(f'Error reading attributes for {classs.__name__}: {e}')

    attributes.sort()
    slots = list(classs.__slots__)
    slots.sort()

    if attributes != slots:
        raise Exception(f'Attributes for {classs.__name__} is\n{attributes} but should be\n{slots}')

    for field in fields:
        if not isinstance(field, str):
            value: Any = None
            if field.is_list:
                value = []
            try:
                actual_value = getattr(instance, field.name)
            except:
                raise Exception(f'{classs.__name__} has no attribute {field.name}')
            if field.name != "latitude" and field.name != "longitude" and value != actual_value:
                raise Exception(
                    f'Invalid default value {classs.__name__}.{field.name} '
                    f'is {actual_value} but should be {value}'
                )
