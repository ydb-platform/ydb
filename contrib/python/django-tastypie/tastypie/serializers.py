import datetime
import json
import re
import io

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.utils.encoding import smart_bytes
from django.core.serializers import json as djangojson

from tastypie.bundle import Bundle
from tastypie.compat import force_str
from tastypie.exceptions import BadRequest, UnsupportedSerializationFormat, \
    UnsupportedDeserializationFormat
from tastypie.utils import format_datetime, format_date, format_time, \
    make_naive


import warnings
try:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        import defusedxml.lxml as lxml
        from defusedxml.common import DefusedXmlException
        from defusedxml.lxml import parse as parse_xml
        from lxml.etree import Element, tostring, LxmlError
except ImportError:
    lxml = None

try:
    import yaml
except ImportError:
    yaml = None

try:
    import biplist
except ImportError:
    biplist = None


XML_ENCODING = re.compile(r'<\?xml.*?\?>', re.IGNORECASE)


# Ugh & blah.
# So doing a regular dump is generally fine, since Tastypie doesn't usually
# serialize advanced types. *HOWEVER*, it will dump out Python Unicode strings
# as a custom YAML tag, which of course ``yaml.safe_load`` can't handle.
if yaml is not None:
    from yaml.constructor import SafeConstructor
    from yaml.loader import Reader, Scanner, Parser, Composer, Resolver

    class TastypieConstructor(SafeConstructor):
        def construct_yaml_unicode_dammit(self, node):
            value = self.construct_scalar(node)
            try:
                return value.encode('ascii')
            except UnicodeEncodeError:
                return value

    TastypieConstructor.add_constructor(
        u'tag:yaml.org,2002:python/unicode',
        TastypieConstructor.construct_yaml_unicode_dammit
    )

    class TastypieLoader(Reader, Scanner, Parser, Composer,
            TastypieConstructor, Resolver):
        def __init__(self, stream):
            Reader.__init__(self, stream)
            Scanner.__init__(self)
            Parser.__init__(self)
            Composer.__init__(self)
            TastypieConstructor.__init__(self)
            Resolver.__init__(self)


def _get_default_formats():
    formats = ['json']
    if lxml:
        formats.append('xml')
    if yaml:
        formats.append('yaml')
    if biplist:
        formats.append('plist')
    return formats


_NUM = 0
_DICT = 1
_LIST = 2
_STR = 3
_BUNDLE = 4
_DATETIME = 5
_DATE = 6
_TIME = 7

_SIMPLETYPES = {
    float: _NUM,
    int: _NUM,
    str: _STR,
    bool: _NUM,
    dict: _DICT,
    list: _LIST,
    tuple: _LIST,
    Bundle: _BUNDLE,
    datetime.datetime: _DATETIME,
    datetime.date: _DATE,
    datetime.time: _TIME,
}


class Serializer(object):
    """
    A swappable class for serialization.

    This handles most types of data as well as the following output formats::

        * json
        * jsonp (Disabled by default)
        * xml
        * yaml
        * plist (see https://bitbucket.org/wooster/biplist)

    It was designed to make changing behavior easy, either by overridding the
    various format methods (i.e. ``to_json``), by changing the
    ``formats/content_types`` options or by altering the other hook methods.
    """

    formats = _get_default_formats()

    content_types = {
        'json': 'application/json',
        'jsonp': 'text/javascript',
        'xml': 'application/xml',
        'yaml': 'text/yaml',
        'plist': 'application/x-plist'
    }

    def __init__(self, formats=None, content_types=None, datetime_formatting=None):
        if datetime_formatting is not None:
            self.datetime_formatting = datetime_formatting
        else:
            self.datetime_formatting = getattr(settings,
                'TASTYPIE_DATETIME_FORMATTING', 'iso-8601')

        self.supported_formats = []

        if content_types is not None:
            self.content_types = content_types

        if formats is not None:
            self.formats = formats

        if self.formats is Serializer.formats and hasattr(settings, 'TASTYPIE_DEFAULT_FORMATS'):
            # We want TASTYPIE_DEFAULT_FORMATS to override unmodified defaults
            # but not intentational changes on Serializer subclasses:
            self.formats = settings.TASTYPIE_DEFAULT_FORMATS

        if not isinstance(self.formats, (list, tuple)):
            raise ImproperlyConfigured(
                'Formats should be a list or tuple, not %r' % self.formats)

        for format in self.formats:
            try:
                self.supported_formats.append(self.content_types[format])
            except KeyError:
                raise ImproperlyConfigured("Content type for specified type '%s' not found. Please provide it at either the class level or via the arguments." % format)

        # Reverse the list, because mimeparse is weird like that. See also
        # https://github.com/django-tastypie/django-tastypie/issues#issue/12 for
        # more information.
        self.supported_formats_reversed = list(self.supported_formats)
        self.supported_formats_reversed.reverse()

        self._from_methods = {}
        self._to_methods = {}

        for short_format, long_format in self.content_types.items():
            method = getattr(self, "from_%s" % short_format, None)

            self._from_methods[long_format] = method

            method = getattr(self, "to_%s" % short_format, None)

            self._to_methods[long_format] = method

    def get_mime_for_format(self, format):
        """
        Given a format, attempts to determine the correct MIME type.

        If not available on the current ``Serializer``, returns
        ``application/json`` by default.
        """
        try:
            return self.content_types[format]
        except KeyError:
            return 'application/json'

    def format_datetime(self, data):
        """
        A hook to control how datetimes are formatted.

        Can be overridden at the ``Serializer`` level (``datetime_formatting``)
        or globally (via ``settings.TASTYPIE_DATETIME_FORMATTING``).

        Default is ``iso-8601``, which looks like "2010-12-16T03:02:14".
        """
        data = make_naive(data)
        if self.datetime_formatting == 'rfc-2822':
            return format_datetime(data)
        if self.datetime_formatting == 'iso-8601-strict':
            # Remove microseconds to strictly adhere to iso-8601
            data = data - datetime.timedelta(microseconds=data.microsecond)

        return data.isoformat()

    def format_date(self, data):
        """
        A hook to control how dates are formatted.

        Can be overridden at the ``Serializer`` level (``datetime_formatting``)
        or globally (via ``settings.TASTYPIE_DATETIME_FORMATTING``).

        Default is ``iso-8601``, which looks like "2010-12-16".
        """
        if self.datetime_formatting == 'rfc-2822':
            return format_date(data)

        return data.isoformat()

    def format_time(self, data):
        """
        A hook to control how times are formatted.

        Can be overridden at the ``Serializer`` level (``datetime_formatting``)
        or globally (via ``settings.TASTYPIE_DATETIME_FORMATTING``).

        Default is ``iso-8601``, which looks like "03:02:14".
        """
        if self.datetime_formatting == 'rfc-2822':
            return format_time(data)
        if self.datetime_formatting == 'iso-8601-strict':
            # Remove microseconds to strictly adhere to iso-8601
            data = (
                datetime.datetime.combine(datetime.date(1, 1, 1), data)
                - datetime.timedelta(microseconds=data.microsecond)
            ).time()

        return data.isoformat()

    def serialize(self, bundle, format='application/json', options=None):
        """
        Given some data and a format, calls the correct method to serialize
        the data and returns the result.
        """
        method = None
        if options is None:
            options = {}

        method = self._to_methods.get(format)

        if method is None:
            raise UnsupportedSerializationFormat(format)

        return method(bundle, options)

    def deserialize(self, content, format='application/json'):
        """
        Given some data and a format, calls the correct method to deserialize
        the data and returns the result.
        """
        method = None

        format = format.split(';')[0]

        method = self._from_methods.get(format)

        if method is None:
            raise UnsupportedDeserializationFormat(format)

        if isinstance(content, bytes):
            content = force_str(content)

        return method(content)

    def to_simple(self, data, options):
        """
        For a piece of data, attempts to recognize it and provide a simplified
        form of something complex.

        This brings complex Python data structures down to native types of the
        serialization format(s).
        """
        if data is None:
            return None

        data_type = type(data)

        stype = _STR

        for dt in data_type.__mro__:
            try:
                stype = _SIMPLETYPES[dt]
                break
            except KeyError:
                pass

        if stype == _NUM:
            return data
        if stype == _DICT:
            to_simple = self.to_simple
            return {key: to_simple(val, options) for key, val in data.items()}
        if stype == _STR:
            return force_str(data)
        if stype == _LIST:
            to_simple = self.to_simple
            return [to_simple(item, options) for item in data]
        if stype == _BUNDLE:
            to_simple = self.to_simple
            return {key: to_simple(val, options) for key, val in data.data.items()}
        if stype == _DATETIME:
            return self.format_datetime(data)
        if stype == _DATE:
            return self.format_date(data)
        if stype == _TIME:
            return self.format_time(data)

    def to_etree(self, data, options=None, name=None, depth=0):
        """
        Given some data, converts that data to an ``etree.Element`` suitable
        for use in the XML output.
        """
        if isinstance(data, (list, tuple)):
            element = Element(name or 'objects')
            if name:
                element = Element(name)
                element.set('type', 'list')
            else:
                element = Element('objects')
            for item in data:
                element.append(self.to_etree(item, options, depth=depth + 1))
                element[:] = sorted(element, key=lambda x: x.tag)
        elif isinstance(data, dict):
            if depth == 0:
                element = Element(name or 'response')
            else:
                element = Element(name or 'object')
                element.set('type', 'hash')
            for (key, value) in data.items():
                element.append(self.to_etree(
                    value, options, name=key, depth=depth + 1))
                element[:] = sorted(element, key=lambda x: x.tag)
        elif isinstance(data, Bundle):
            element = Element(name or 'object')
            for field_name, field_object in data.data.items():
                element.append(self.to_etree(
                    field_object, options, name=field_name, depth=depth + 1))
                element[:] = sorted(element, key=lambda x: x.tag)
        else:
            element = Element(name or 'value')
            simple_data = self.to_simple(data, options)
            data_type = get_type_string(simple_data)

            if data_type != 'string':
                element.set('type', get_type_string(simple_data))

            if data_type != 'null':
                if isinstance(simple_data, str):
                    element.text = simple_data
                else:
                    element.text = force_str(simple_data)

        return element

    def from_etree(self, data):
        """
        Not the smartest deserializer on the planet. At the request level,
        it first tries to output the deserialized subelement called "object"
        or "objects" and falls back to deserializing based on hinted types in
        the XML element attribute "type".
        """
        if data.tag == 'request':
            # if "object" or "objects" exists, return deserialized forms.
            elements = data.getchildren()
            for element in elements:
                if element.tag in ('object', 'objects'):
                    return self.from_etree(element)
            return {
                element.tag: self.from_etree(element)
                for element in elements
            }
        elif data.tag == 'object' or data.get('type') == 'hash':
            return {
                element.tag: self.from_etree(element)
                for element in data.getchildren()
            }
        elif data.tag == 'objects' or data.get('type') == 'list':
            return [self.from_etree(element) for element in data.getchildren()]
        else:
            type_string = data.get('type')
            if type_string in ('string', None):
                return data.text
            elif type_string == 'integer':
                return int(data.text)
            elif type_string == 'float':
                return float(data.text)
            elif type_string == 'boolean':
                if data.text == 'True':
                    return True
                else:
                    return False
            else:
                return None

    def to_json(self, data, options=None):
        """
        Given some Python data, produces JSON output.
        """
        options = options or {}
        data = self.to_simple(data, options)

        return djangojson.json.dumps(data, cls=djangojson.DjangoJSONEncoder,
            sort_keys=True, ensure_ascii=False)

    def from_json(self, content):
        """
        Given some JSON data, returns a Python dictionary of the decoded data.
        """
        try:
            return json.loads(content)
        except ValueError:
            raise BadRequest('Request is not valid JSON.')

    def to_jsonp(self, data, options=None):
        """
        Given some Python data, produces JSON output wrapped in the provided
        callback.

        Due to a difference between JSON and Javascript, two
        newline characters, \u2028 and \u2029, need to be escaped.
        See http://timelessrepo.com/json-isnt-a-javascript-subset for
        details.
        """
        options = options or {}
        jsonstr = self.to_json(data, options).replace(
            u'\u2028', u'\\u2028').replace(u'\u2029', u'\\u2029')
        return u'%s(%s)' % (options['callback'], jsonstr)

    def to_xml(self, data, options=None):
        """
        Given some Python data, produces XML output.
        """
        options = options or {}

        if lxml is None:
            raise ImproperlyConfigured(
                "Usage of the XML aspects requires lxml and defusedxml.")

        return tostring(self.to_etree(data, options), xml_declaration=True,
            encoding='utf-8')

    def from_xml(self, content, forbid_dtd=True, forbid_entities=True):
        """
        Given some XML data, returns a Python dictionary of the decoded data.

        By default XML entity declarations and DTDs will raise a BadRequest
        exception content but subclasses may choose to override this if
        necessary.
        """
        if lxml is None:
            raise ImproperlyConfigured(
                "Usage of the XML aspects requires lxml and defusedxml.")

        try:
            # Stripping the encoding declaration. Because lxml.
            # See http://lxml.de/parsing.html, "Python unicode strings".
            content = XML_ENCODING.sub('', content)
            parsed = parse_xml(
                io.StringIO(content),
                forbid_dtd=forbid_dtd,
                forbid_entities=forbid_entities
            )
        except (LxmlError, DefusedXmlException):
            raise BadRequest()

        return self.from_etree(parsed.getroot())

    def to_yaml(self, data, options=None):
        """
        Given some Python data, produces YAML output.
        """
        options = options or {}

        if yaml is None:
            raise ImproperlyConfigured(
                "Usage of the YAML aspects requires yaml.")

        return yaml.dump(self.to_simple(data, options))

    def from_yaml(self, content):
        """
        Given some YAML data, returns a Python dictionary of the decoded data.
        """
        if yaml is None:
            raise ImproperlyConfigured(
                "Usage of the YAML aspects requires yaml.")

        return yaml.load(content, Loader=TastypieLoader)

    def to_plist(self, data, options=None):
        """
        Given some Python data, produces binary plist output.
        """
        options = options or {}

        if biplist is None:
            raise ImproperlyConfigured(
                "Usage of the plist aspects requires biplist.")

        return biplist.writePlistToString(self.to_simple(data, options))

    def from_plist(self, content):
        """
        Given some binary plist data, returns a Python dictionary of the
        decoded data.
        """
        if biplist is None:
            raise ImproperlyConfigured(
                "Usage of the plist aspects requires biplist.")

        if isinstance(content, str):
            content = smart_bytes(content)

        return biplist.readPlistFromString(content)


def get_type_string(data):
    """
    Translates a Python data type into a string format.
    """
    data_type = type(data)

    if data_type == int:
        return 'integer'
    elif data_type == float:
        return 'float'
    elif data_type == bool:
        return 'boolean'
    elif data_type in (list, tuple):
        return 'list'
    elif data_type == dict:
        return 'hash'
    elif data is None:
        return 'null'
    elif isinstance(data, str):
        return 'string'
