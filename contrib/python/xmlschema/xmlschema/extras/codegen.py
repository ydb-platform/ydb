#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
# mypy: ignore-errors
"""
This module contains abstact base class and helper
functions for building XSD based code generators.
"""
import importlib.resources
import importlib.util
import os
import re
import sys
import inspect
import logging
from abc import ABC, ABCMeta
from fnmatch import fnmatch
from pathlib import Path
from typing import Optional

from jinja2 import Environment, ChoiceLoader, FileSystemLoader, \
    TemplateNotFound, TemplateAssertionError
from elementpath import datatypes

import xmlschema
from xmlschema.validators import XsdType, XsdElement, XsdAttribute
from xmlschema.names import XSD_NAMESPACE


NCNAME_PATTERN = re.compile(r'^[^\d\W][\w.\-]*$')
QNAME_PATTERN = re.compile(
    r'^(?:(?P<prefix>[^\d\W][\w\-.\xb7\u0387\u06DD\u06DE]*):)?'
    r'(?P<local>[^\d\W][\w\-.\xb7\u0387\u06DD\u06DE]*)$',
)


def is_shell_wildcard(pathname):
    return '*' in pathname or '?' in pathname or '[' in pathname


def xsd_qname(name):
    return f'{{{XSD_NAMESPACE}}}{name}'


def filter_method(func):
    """Marks a method for registration as template filter."""
    func.is_filter = True
    return func


def test_method(func):
    """Marks a method for registration as template test."""
    func.is_test = True
    return func


logger = logging.getLogger('xmlschema-codegen')


class GeneratorMeta(ABCMeta):
    """Metaclass for creating code generators. Checks formal_language """

    def __new__(mcs, name, bases, attrs):
        module = sys.modules.get(attrs['__module__'])
        module_spec = importlib.util.find_spec(module.__name__)
        module_path = module_spec.origin if module_spec and module_spec.origin else os.getcwd()

        formal_language = None
        searchpaths = []
        builtin_types = {}

        for base in bases:
            if getattr(base, 'formal_language', None):
                if formal_language is None:
                    formal_language = base.formal_language
                elif formal_language != base.formal_language:
                    raise ValueError("ambiguous formal_language from base classes")

            if getattr(base, 'searchpaths', None):
                searchpaths.extend(base.searchpaths)
            if getattr(base, 'builtin_types', None):
                builtin_types.update(base.builtin_types)

        if 'formal_language' not in attrs:
            attrs['formal_language'] = formal_language
        elif formal_language and formal_language != attrs['formal_language']:
            raise ValueError("formal_language cannot be changed")

        try:
            for path in attrs['searchpaths']:
                if Path(path).is_absolute():
                    dirpath = Path(path)
                else:
                    dirpath = Path(module_path).parent.joinpath(path)

                if not dirpath.is_dir():
                    raise ValueError(f"path {str(path)!r} is not a directory!")
                searchpaths.append(dirpath)

        except (KeyError, TypeError):
            pass
        else:
            attrs['searchpaths'] = searchpaths

        try:
            for k, v in attrs['builtin_types'].items():
                builtin_types[xsd_qname(k)] = v
        except (KeyError, AttributeError):
            pass
        finally:
            attrs['builtin_types'] = builtin_types

        return type.__new__(mcs, name, bases, attrs)


class AbstractGenerator(ABC, metaclass=GeneratorMeta):
    """
    Abstract base class for code generators based on Jinja2 template engine.

    :param schema: the source or the instance of the XSD schema.
    :param searchpath: additional search path for custom templates. \
    If provided the search path has priority over searchpaths defined \
    in generator class.
    :param types_map: a dictionary with custom mapping for XSD types.
    """
    formal_language: Optional[str] = None
    """The formal language associated to the code generator (eg. Python)."""

    searchpaths: Optional[list[str]] = None
    """
    Directory paths for searching templates, specified with a list or a tuple.
    Each path must be provided as relative from the directory of the module
    where the class is defined. Extends the searchpath defined in base classes.
    """

    builtin_types = {
        'anyType': '',
        'anySimpleType': '',
    }
    """
    Translation map for XSD builtin types. Updates the builtin_types
    defined in base classes.
    """

    def __init__(self, schema, searchpath=None, types_map=None):
        if isinstance(schema, xmlschema.XMLSchemaBase):
            self.schema = schema
        else:
            self.schema = xmlschema.XMLSchema11(schema)

        file_loaders = []
        if searchpath:
            file_loaders.append(FileSystemLoader(searchpath))
        if self.searchpaths is not None:
            file_loaders.extend(
                FileSystemLoader(str(path)) for path in reversed(self.searchpaths)
            )
        if not file_loaders:
            raise ValueError("no search paths defined!")
        loader = ChoiceLoader(file_loaders) if len(file_loaders) > 1 else file_loaders[0]

        self.types_map = self.builtin_types.copy()
        if types_map:
            if not self.schema.target_namespace:
                self.types_map.update(types_map)
            else:
                ns_part = '{%s}' % self.schema.target_namespace
                self.types_map.update((ns_part + k, v) for k, v in types_map.items())

        self.filters = {}
        self.tests = {}
        for name in filter(lambda x: callable(getattr(self, x)), dir(self)):
            method = getattr(self, name)
            if inspect.isfunction(method):
                # static methods
                if getattr(method, 'is_filter', False):
                    self.filters[name] = method
                elif getattr(method, 'is_test', False):
                    self.tests[name] = method
            elif inspect.isroutine(method) and hasattr(method, '__func__'):
                # class and instance methods
                if getattr(method.__func__, 'is_filter', False):
                    self.filters[name] = method
                elif getattr(method.__func__, 'is_test', False):
                    self.tests[name] = method

        type_mapping_filter = f'{self.formal_language}_type'.lower().replace(' ', '_')
        if type_mapping_filter not in self.filters:
            self.filters[type_mapping_filter] = self.map_type

        self._env = Environment(loader=loader)
        self._env.filters.update(self.filters)
        self._env.tests.update(self.tests)

    def __repr__(self):
        if self.schema.url:
            return f'{self.__class__.__name__}(schema={self.schema.name!r})'
        return f'{self.__class__.__name__}(namespace={self.schema.target_namespace!r})'

    def list_templates(self, extensions=None, filter_func=None):
        return self._env.list_templates(extensions, filter_func)

    def matching_templates(self, name):
        return self._env.list_templates(filter_func=lambda x: fnmatch(x, name))

    def get_template(self, name, parent=None, global_vars=None):
        return self._env.get_template(name, parent, global_vars)

    def select_template(self, names, parent=None, global_vars=None):
        return self._env.select_template(names, parent, global_vars)

    def render(self, names, parent=None, global_vars=None):
        if isinstance(names, str):
            names = [names]
        elif not all(isinstance(x, str) for x in names):
            raise TypeError("'names' argument must contain only strings!")

        results = []
        for name in names:
            try:
                template = self._env.get_template(name, parent, global_vars)
            except TemplateNotFound as err:
                logger.debug("name %r: %s", name, str(err))
            except TemplateAssertionError as err:
                logger.warning("template %r: %s", name, str(err))
            else:
                results.append(template.render(schema=self.schema))
        return results

    def render_to_files(self, names, parent=None, global_vars=None, output_dir='.', force=False):
        if isinstance(names, str):
            names = [names]
        elif not all(isinstance(x, str) for x in names):
            raise TypeError("'names' argument must contain only strings!")

        template_names = []
        for name in names:
            if is_shell_wildcard(name):
                template_names.extend(self.matching_templates(name))
            else:
                template_names.append(name)

        output_dir = Path(output_dir)
        rendered = []

        for name in template_names:
            try:
                template = self._env.get_template(name, parent, global_vars)
            except TemplateNotFound as err:
                logger.debug("name %r: %s", name, str(err))
            except TemplateAssertionError as err:
                logger.warning("template %r: %s", name, str(err))
            else:
                output_file = output_dir.joinpath(Path(name).name).with_suffix('')
                if not force and output_file.exists():
                    continue

                result = template.render(schema=self.schema)
                logger.info("write file %r", str(output_file))
                with open(output_file, 'w') as fp:
                    fp.write(result)
                rendered.append(str(output_file))

        return rendered

    def map_type(self, obj):
        """
        Maps an XSD type to a type declaration of the target language.
        This method is registered as filter with a name dependant from
        the language name (eg. c_type).

        :param obj: an XSD type or another type-related declaration as \
        an attribute or an element.
        :return: an empty string for non-XSD objects.
        """
        if isinstance(obj, XsdType):
            xsd_type = obj
        elif isinstance(obj, (XsdAttribute, XsdElement)):
            xsd_type = obj.type
        else:
            return ''

        try:
            return self.types_map[xsd_type.name]
        except KeyError:
            try:
                return self.types_map[xsd_type.base_type.name]
            except (KeyError, AttributeError):
                if xsd_type.is_complex():
                    return self.types_map[xsd_qname('anyType')]
                else:
                    return self.types_map[xsd_qname('anySimpleType')]

    @staticmethod
    @filter_method
    def name(obj, unnamed='none'):
        """
        Get the unqualified name of the provided object. Invalid
        chars for identifiers are replaced by an underscore.

        :param obj: an XSD object or a named object or a string.
        :param unnamed: value for unnamed objects. Defaults to 'none'.
        :return: str
        """
        try:
            name = obj.local_name
        except AttributeError:
            try:
                obj = obj.name
            except AttributeError:
                pass

            if not isinstance(obj, str):
                return unnamed

            try:
                if obj[0] == '{':
                    _, name = obj.split('}')
                elif ':' in obj:
                    prefix, name = obj.split(':')
                    if NCNAME_PATTERN.match(prefix) is None:
                        return ''
                else:
                    name = obj
            except (IndexError, ValueError):
                return ''
        else:
            if not isinstance(name, str):
                return ''

        if NCNAME_PATTERN.match(name) is None:
            return unnamed
        return name.replace('.', '_').replace('-', '_')

    @filter_method
    def qname(self, obj, unnamed='none', sep='__'):
        """
        Get the QName of the provided object. Invalid chars for
        identifiers are replaced by an underscore.

        :param obj: an XSD object or a named object or a string.
        :param unnamed: value for unnamed objects. Defaults to 'none'.
        :param sep: the replacement for colon. Defaults to double underscore.
        :return: str
        """
        try:
            qname = obj.prefixed_name
        except AttributeError:
            try:
                obj = obj.name
            except AttributeError:
                pass

            if not isinstance(obj, str):
                return unnamed

            try:
                if obj[0] == '{':
                    namespace, local_name = obj[1:].split('}')
                    for prefix, uri in self.schema.namespaces.items():
                        if uri == namespace:
                            qname = f'{prefix}:{local_name}'
                            break
                    else:
                        qname = local_name
                else:
                    qname = obj
            except IndexError:
                return ''
            except ValueError:
                return unnamed

        if not qname or QNAME_PATTERN.match(qname) is None:
            return unnamed
        return qname.replace('.', '_').replace('-', '_').replace(':', sep)

    @filter_method
    def namespace(self, obj):
        """Get the namespace URI of the provided object."""
        try:
            namespace = obj.target_namespace
        except AttributeError:
            if isinstance(obj, datatypes.QName):
                return obj.namespace
            elif not isinstance(obj, str):
                return ''

            try:
                if obj[0] == '{':
                    namespace, _ = obj[1:].split('}')
                    return namespace
                elif ':' in obj:
                    prefix, _ = obj.split(':')
                    return self.schema.namespaces.get(prefix, '')
                else:
                    return ''
            except (IndexError, ValueError):
                return ''
        else:
            return namespace if isinstance(namespace, str) else ''

    @staticmethod
    @filter_method
    def type_name(obj, suffix=None, unnamed='none'):
        """
        Get the unqualified name of the XSD type. Invalid
        chars for identifiers are replaced by an underscore.

        :param obj: an instance of (XsdType|XsdAttribute|XsdElement).
        :param suffix: force a suffix. For default removes '_type' or 'Type' suffixes.
        :param unnamed: value for unnamed XSD types. Defaults to 'none'.
        :return: str
        """
        if isinstance(obj, XsdType):
            name = obj.local_name or unnamed
        elif isinstance(obj, (XsdElement, XsdAttribute)):
            name = obj.type.local_name or unnamed
        else:
            name = unnamed

        if name.endswith('Type'):
            name = name[:-4]
        elif name.endswith('_type'):
            name = name[:-5]

        if suffix:
            name = f'{name}{suffix}'

        return name.replace('.', '_').replace('-', '_')

    @staticmethod
    @filter_method
    def type_qname(obj, suffix=None, unnamed='none', sep='__'):
        """
        Get the unqualified name of the XSD type. Invalid
        chars for identifiers are replaced by an underscore.

        :param obj: an instance of (XsdType|XsdAttribute|XsdElement).
        :param suffix: force a suffix. For default removes '_type' or 'Type' suffixes.
        :param unnamed: value for unnamed XSD types. Defaults to 'none'.
        :param sep: the replacement for colon. Defaults to double underscore.
        :return: str
        """
        if isinstance(obj, XsdType):
            qname = obj.prefixed_name or unnamed
        elif isinstance(obj, (XsdElement, XsdAttribute)):
            qname = obj.type.prefixed_name or unnamed
        else:
            qname = unnamed

        if qname.endswith('Type'):
            qname = qname[:-4]
        elif qname.endswith('_type'):
            qname = qname[:-5]

        if suffix:
            qname = f'{qname}{suffix}'

        return qname.replace('.', '_').replace('-', '_').replace(':', sep)

    @staticmethod
    @filter_method
    def sort_types(xsd_types, accept_circularity=False):
        """
        Returns a sorted sequence of XSD types usable for building type declarations.

        :param xsd_types: a sequence with XSD types.
        :param accept_circularity: if set to `True` circularities \
        are accepted. Defaults to `False`.
        :return: a list with ordered types.
        """
        if not isinstance(xsd_types, (list, tuple)):
            try:
                xsd_types = list(xsd_types.values())
            except AttributeError:
                pass

        assert all(isinstance(x, XsdType) for x in xsd_types)
        ordered_types = [x for x in xsd_types if x.is_simple()]
        ordered_types.extend(x for x in xsd_types if x.is_complex() and x.has_simple_content())
        unordered = {x: [] for x in xsd_types if x.is_complex() and not x.has_simple_content()}

        for xsd_type in unordered:
            for e in xsd_type.content.iter_elements():
                if e.type in unordered:
                    unordered[xsd_type].append(e.type)

        while unordered:
            deleted = 0
            for xsd_type in xsd_types:
                if xsd_type in unordered:
                    if not unordered[xsd_type]:
                        del unordered[xsd_type]
                        ordered_types.append(xsd_type)
                        deleted += 1

            for xsd_type in unordered:
                unordered[xsd_type] = [x for x in unordered[xsd_type] if x in unordered]

            if not deleted:
                if not accept_circularity:
                    raise ValueError(f"circularity found between {list(unordered)!r}")
                ordered_types.extend(list(unordered))
                break

        assert len(xsd_types) == len(ordered_types)
        return ordered_types

    def is_derived(self, xsd_type, *names, derivation=None):
        """
        Returns `True` if the argument XSD type is derived from any
        of other types expressed by name, otherwise returns `False`.

        :param xsd_type: an XsdComplexType/XsdSimpleType instance.
        :param names: positional argument with the names of other \
        XSD types.
        :param derivation: the type of derivation, that can be \
        *extension* or *restriction*, or both with a space separator. \
        If no value is provided it only checks if it is derived from \
        or if it is the XSD type itself.
        """
        for type_name in names:
            if not isinstance(type_name, str) or not type_name:
                continue  # pragma: no cover
            elif type_name[0] == '{':
                other = self.schema.maps.types.get(type_name)
            else:
                try:
                    expanded_name = self.schema.resolve_qname(type_name)
                except xmlschema.XMLSchemaException:
                    other = self.schema.types.get(type_name)
                else:
                    other = self.schema.maps.types.get(expanded_name)
                    if other is None:
                        other = self.schema.types.get(type_name)

            if other is not None and xsd_type.is_derived(other, derivation):
                return True

        return False

    @test_method
    def derivation(self, xsd_type, *names):
        return self.is_derived(xsd_type, *names)

    @test_method
    def extension(self, xsd_type, *names):
        return self.is_derived(xsd_type, *names, derivation='extension')

    @test_method
    def restriction(self, xsd_type, *names):
        return self.is_derived(xsd_type, *names, derivation='restriction')

    @staticmethod
    @test_method
    def multi_sequence(xsd_type):
        try:
            return any(e.is_multiple() for e in xsd_type.content.iter_elements())
        except AttributeError:
            return False


class PythonGenerator(AbstractGenerator):
    """A Python code generator for XSD schemas."""

    formal_language = 'Python'

    searchpaths = [importlib.resources.files(__package__) / "templates/python"]

    builtin_types = {
        'string': 'str',
        'decimal': 'decimal.Decimal',
        'float': 'float',
        'double': 'float',

        'date': 'datatypes.Date10',
        'dateTime': 'datatypes.DateTime10',
        'gDay': 'datatypes.GregorianDay',
        'gMonth': 'datatypes.GregorianMonth',
        'gMonthDay': 'datatypes.GregorianMonthDay',
        'gYear': 'datatypes.GregorianYear10',
        'gYearMonth': 'datatypes.GregorianYearMonth10',
        'time': 'datatypes.Time',
        'duration': 'datatypes.Duration',

        'QName': 'datatypes.QName',
        'NOTATION': 'datatypes.DateTime10',
        'anyURI': 'datatypes.AnyURI',
        'boolean': 'bool',
        'base64Binary': 'datatypes.Base64Binary',
        'hexBinary': 'datatypes.HexBinary',
        'normalizedString': 'str',
        'token': 'str',
        'language': 'str',
        'Name': 'str',
        'NCName': 'str',
        'ID': 'str',
        'IDREF': 'str',
        'ENTITY': 'str',
        'NMTOKEN': 'str',

        'integer': 'int',
        'long': 'int',
        'int': 'int',
        'short': 'int',
        'byte': 'int',
        'nonNegativeInteger': 'int',
        'positiveInteger': 'int',
        'unsignedLong': 'int',
        'unsignedInt': 'int',
        'unsignedShort': 'int',
        'unsignedByte': 'int',
        'nonPositiveInteger': 'int',
        'negativeInteger': 'int',

        # XSD 1.1 built-in types
        'dateTimeStamp': 'datatypes.DateTimeStamp10',
        'dayTimeDuration': 'datatypes.DayTimeDuration',
        'yearMonthDuration': 'datatypes.YearMonthDuration',
    }
