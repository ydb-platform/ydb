#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import json
import dataclasses as dc
from io import IOBase, TextIOBase
from collections.abc import Iterator
from functools import partial
from typing import Any, BinaryIO, IO, Optional, TextIO, Union
from xml.etree import ElementTree

from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError, XMLResourceError
from xmlschema.names import XSD_NAMESPACE, XSI_TYPE, XSD_SCHEMA
from xmlschema.aliases import ElementType, NsmapType, LocationsType, SourceArgType, \
    DecodeType, EncodeType, JsonDecodeType, XMLSourceType, SchemaType
from xmlschema.translation import gettext as _
from xmlschema.arguments import Argument, validate_type, BooleanOption, ValidationOption
from xmlschema.utils.etree import is_etree_document, etree_tostring
from xmlschema.utils.qnames import get_extended_qname, update_namespaces, get_namespace_map
from xmlschema.resources import fetch_schema_locations, XMLResource
from xmlschema.converters import ConverterType
from xmlschema.validators import XMLSchema10, XMLSchemaBase, XMLSchemaValidationError
from xmlschema.arguments import LocationsOption
from xmlschema.settings import ResourceSettings, SchemaSettings

__all__ = ('from_json', 'is_valid', 'iter_errors', 'iter_decode', 'to_dict',
           'to_etree', 'to_json', 'validate', 'XmlDocument')

RESOURCE_KWARGS = frozenset(fld.name for fld in dc.fields(ResourceSettings))
SCHEMA_KWARGS = frozenset(fld.name for fld in dc.fields(SchemaSettings))


class SchemaArgument(Argument[SchemaType]):
    _validators = partial(validate_type, types=XMLSchemaBase),

    def __set__(self, instance: Any, value: Any) -> None:
        setattr(instance, self._name, self.validated_value(value))


def get_context(xml_document: Union[XMLSourceType, XMLResource],
                schema: Optional[Union[XMLSchemaBase, SourceArgType]] = None,
                cls: Optional[type[XMLSchemaBase]] = None,
                **kwargs: Any) -> tuple[XMLResource, SchemaType]:
    """
    Get the XML document validation/decode context.

    :return: an XMLResource instance and a schema instance.
    """
    resource: XMLResource
    if not isinstance(xml_document, XMLResource):
        _kwargs = {k: kwargs[k] for k in kwargs if k in RESOURCE_KWARGS}
        resource = XMLResource(xml_document, **_kwargs)
    elif not isinstance(xml_document, XmlDocument):
        resource = xml_document
    else:
        return xml_document, xml_document.schema

    _kwargs = {k: kwargs[k] for k in kwargs if k in SCHEMA_KWARGS}
    return resource, get_resource_schema(resource, schema, cls, **_kwargs)


def get_resource_schema(resource: XMLResource,
                        schema: Optional[Union[XMLSchemaBase, SourceArgType]] = None,
                        cls: Optional[type[XMLSchemaBase]] = None,
                        validation: str = 'strict',
                        locations: Optional[LocationsType] = None,
                        use_location_hints: bool = True,
                        **kwargs: Any) -> SchemaType:
    if cls is None:
        cls = XMLSchema10
    elif not issubclass(cls, XMLSchemaBase):
        raise XMLSchemaTypeError(_("invalid schema class {!r}").format(cls))

    if isinstance(schema, XMLSchemaBase) and resource.namespace in schema.maps.namespaces:
        return schema

    if use_location_hints:
        try:
            schema_location, locations = fetch_schema_locations(resource, locations, **kwargs)
        except ValueError:
            pass
        else:
            kwargs['locations'] = locations
            if schema is None or isinstance(schema, XMLSchemaBase):
                return cls(schema_location, **kwargs)
            else:
                return cls(schema, **kwargs)

    if isinstance(schema, XMLSchemaBase):
        return schema  # fallback to a schema for a different namespace
    elif schema is not None:
        return cls(schema, locations=locations, **kwargs)
    elif XSD_NAMESPACE == resource.namespace:
        assert cls.meta_schema is not None
        return cls.meta_schema
    elif validation == 'skip' or XSI_TYPE in resource.root.attrib:
        return get_dummy_schema(resource.root.tag, cls)
    else:
        msg = _("cannot get a schema for XML data, provide a schema argument")
        raise XMLSchemaValueError(msg)


def get_dummy_schema(tag: str, cls: type[XMLSchemaBase]) -> XMLSchemaBase:
    if tag.startswith('{'):
        namespace, name = tag[1:].split('}')
    else:
        namespace, name = '', tag

    if namespace:
        return cls(
            '<xs:schema xmlns:xs="{}" targetNamespace="{}">\n'
            '    <xs:element name="{}"/>\n'
            '</xs:schema>'.format(XSD_NAMESPACE, namespace, name)
        )
    else:
        return cls(
            '<xs:schema xmlns:xs="{}">\n'
            '    <xs:element name="{}"/>\n'
            '</xs:schema>'.format(XSD_NAMESPACE, name)
        )


def get_lazy_json_encoder(errors: list[XMLSchemaValidationError]) -> type[json.JSONEncoder]:

    class JSONLazyEncoder(json.JSONEncoder):
        def default(self, obj: Any) -> Any:
            if isinstance(obj, Iterator):
                for result in obj:
                    if isinstance(result, XMLSchemaValidationError):
                        errors.append(result)
                    else:
                        return result
                return None
            return json.JSONEncoder.default(self, obj)

    return JSONLazyEncoder


def validate(xml_document: Union[XMLSourceType, XMLResource],
             schema: Optional[XMLSchemaBase] = None,
             cls: Optional[type[XMLSchemaBase]] = None,
             path: Optional[str] = None,
             schema_path: Optional[str] = None,
             use_defaults: bool = True,
             namespaces: Optional[NsmapType] = None,
             locations: Optional[LocationsType] = None,
             use_location_hints: bool = True,
             **kwargs: Any) -> None:
    """
    Validates an XML document against a schema instance. This function builds an
    :class:`XMLSchema` object for validating the XML document. Raises an
    :exc:`XMLSchemaValidationError` if the XML document is not validated against
    the schema.

    :param xml_document: can be an :class:`XMLResource` instance, a file-like object a path \
    to a file or a URI of a resource or an Element instance or an ElementTree instance or \
    a string containing the XML data. If the passed argument is not an :class:`XMLResource` \
    instance a new one is built using this and *defuse*, *timeout* and *lazy* arguments.
    :param schema: can be a schema instance or a file-like object or a file path or a URL \
    of a resource or a string containing the schema.
    :param cls: class to use for building the schema instance (for default \
    :class:`XMLSchema10` is used).
    :param path: is an optional XPath expression that matches the elements of the XML \
    data that have to be decoded. If not provided the XML root element is used.
    :param schema_path: an XPath expression to select the XSD element to use for decoding. \
    If not provided the *path* argument or the *source* root tag are used.
    :param use_defaults: defines when to use element and attribute defaults for filling \
    missing required values.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    :param locations: additional schema location hints, used if a schema instance \
    has to be built.
    :param use_location_hints: for default, in case a schema instance has \
    to be built, uses also schema locations hints provided within XML data. \
    set this option to `False` to ignore these schema location hints.
    :param kwargs: other optional arguments for building :class:`XMLResource` or \
    :class:`XMLSchema` instances provided as keyword arguments.
    """
    kwargs.update(locations=locations, use_location_hints=use_location_hints)
    source, schema = get_context(xml_document, schema, cls, **kwargs)
    schema.validate(source, path, schema_path, use_defaults, namespaces,
                    use_location_hints=use_location_hints)


def is_valid(xml_document: Union[XMLSourceType, XMLResource],
             schema: Optional[XMLSchemaBase] = None,
             cls: Optional[type[XMLSchemaBase]] = None,
             path: Optional[str] = None,
             schema_path: Optional[str] = None,
             use_defaults: bool = True,
             namespaces: Optional[NsmapType] = None,
             locations: Optional[LocationsType] = None,
             use_location_hints: bool = True,
             **kwargs: Any) -> bool:
    """
    Like :meth:`validate` except that do not raise an exception but returns ``True`` if
    the XML document is valid, ``False`` if it's invalid.
    """
    kwargs.update(validation='lax', locations=locations, use_location_hints=use_location_hints)
    source, schema = get_context(xml_document, schema, cls, **kwargs)
    return schema.is_valid(source, path, schema_path, use_defaults, namespaces,
                           use_location_hints=use_location_hints)


def iter_errors(xml_document: Union[XMLSourceType, XMLResource],
                schema: Optional[XMLSchemaBase] = None,
                cls: Optional[type[XMLSchemaBase]] = None,
                path: Optional[str] = None,
                schema_path: Optional[str] = None,
                use_defaults: bool = True,
                namespaces: Optional[NsmapType] = None,
                locations: Optional[LocationsType] = None,
                use_location_hints: bool = True,
                **kwargs: Any) -> Iterator[XMLSchemaValidationError]:
    """
    Creates an iterator for the errors generated by the validation of an XML document.
    Takes the same arguments of the function :meth:`validate`.
    """
    kwargs.update(validation='lax', locations=locations, use_location_hints=use_location_hints)
    source, schema = get_context(xml_document, schema, cls, **kwargs)
    return schema.iter_errors(source, path, schema_path, use_defaults, namespaces,
                              use_location_hints=use_location_hints)


def iter_decode(xml_document: Union[XMLSourceType, XMLResource],
                schema: Optional[XMLSchemaBase] = None,
                cls: Optional[type[XMLSchemaBase]] = None,
                path: Optional[str] = None,
                validation: str = 'lax',
                locations: Optional[LocationsType] = None,
                use_location_hints: bool = True,
                **kwargs: Any) -> Iterator[Union[Any, XMLSchemaValidationError]]:
    """
    Creates an iterator for decoding an XML source to a data structure. For default
    the document is validated during the decoding phase and if it's invalid then one
    or more :exc:`XMLSchemaValidationError` instances are yielded before the decoded data.

    :param xml_document: can be an :class:`XMLResource` instance, a file-like object a path \
    to a file or a URI of a resource or an Element instance or an ElementTree instance or \
    a string containing the XML data. If the passed argument is not an :class:`XMLResource` \
    instance a new one is built using this and *defuse*, *timeout* and *lazy* arguments.
    :param schema: can be a schema instance or a file-like object or a file path or a URL \
    of a resource or a string containing the schema.
    :param cls: class to use for building the schema instance (for default uses \
    :class:`XMLSchema10`).
    :param path: is an optional XPath expression that matches the elements of the XML \
    data that have to be decoded. If not provided the XML root element is used.
    :param validation: defines the XSD validation mode to use for decode, can be \
    'strict', 'lax' or 'skip'.
    :param locations: additional schema location hints, in case a schema instance \
    has to be built.
    :param use_location_hints: for default, in case a schema instance has \
    to be built, uses also schema locations hints provided within XML data. \
    set this option to `False` to ignore these schema location hints.
    :param kwargs: other optional arguments of :meth:`XMLSchemaBase.iter_decode` \
    or for building :class:`XMLResource` or :class:`XMLSchema` instances provided \
    as keyword arguments.
    :raises: :exc:`XMLSchemaValidationError` if the XML document is invalid and \
    ``validation='strict'`` is provided.
    """
    kwargs.update(
        validation=validation,
        locations=locations,
        use_location_hints=use_location_hints
    )
    source, _schema = get_context(xml_document, schema, cls, **kwargs)
    yield from _schema.iter_decode(source, path=path, **kwargs)


def to_dict(xml_document: Union[XMLSourceType, XMLResource],
            schema: Optional[XMLSchemaBase] = None,
            cls: Optional[type[XMLSchemaBase]] = None,
            path: Optional[str] = None,
            validation: str = 'strict',
            locations: Optional[LocationsType] = None,
            use_location_hints: bool = True,
            **kwargs: Any) -> DecodeType[Any]:
    """
    Decodes an XML document to a Python's nested dictionary. Takes the same arguments
    of the function :meth:`iter_decode`, but *validation* mode defaults to 'strict'.

    :return: an object containing the decoded data. If ``validation='lax'`` is provided \
    validation errors are collected and returned in a tuple with the decoded data.
    :raises: :exc:`XMLSchemaValidationError` if the XML document is invalid and \
    ``validation='strict'`` is provided.
    """
    kwargs.update(
        validation=validation,
        locations=locations,
        use_location_hints=use_location_hints
    )
    source, _schema = get_context(xml_document, schema, cls, **kwargs)
    return _schema.decode(source, path=path, **kwargs)


def to_json(xml_document: Union[XMLSourceType, XMLResource],
            fp: Optional[IO[str]] = None,
            schema: Optional[XMLSchemaBase] = None,
            cls: Optional[type[XMLSchemaBase]] = None,
            path: Optional[str] = None,
            validation: str = 'strict',
            locations: Optional[LocationsType] = None,
            use_location_hints: bool = True,
            json_options: Optional[dict[str, Any]] = None,
            **kwargs: Any) -> JsonDecodeType:
    """
    Serialize an XML document to JSON. For default the XML data is validated during
    the decoding phase. Raises an :exc:`XMLSchemaValidationError` if the XML document
    is not validated against the schema.

    :param xml_document: can be an :class:`XMLResource` instance, a file-like object a path \
    to a file or a URI of a resource or an Element instance or an ElementTree instance or \
    a string containing the XML data. If the passed argument is not an :class:`XMLResource` \
    instance a new one is built using this and *defuse*, *timeout* and *lazy* arguments.
    :param fp: can be a :meth:`write()` supporting file-like object.
    :param schema: can be a schema instance or a file-like object or a file path or a URL \
    of a resource or a string containing the schema.
    :param cls: schema class to use for building the instance (for default uses \
    :class:`XMLSchema10`).
    :param path: is an optional XPath expression that matches the elements of the XML \
    data that have to be decoded. If not provided the XML root element is used.
    :param validation: defines the XSD validation mode to use for decode, can be \
    'strict', 'lax' or 'skip'.
    :param locations: additional schema location hints, in case the schema instance \
    has to be built.
    :param use_location_hints: for default, in case a schema instance has \
    to be built, uses also schema locations hints provided within XML data. \
    set this option to `False` to ignore these schema location hints.
    :param json_options: a dictionary with options for the JSON serializer.
    :param kwargs: optional arguments of :meth:`XMLSchemaBase.iter_decode` as keyword arguments \
    to variate the decoding process.
    :return: a string containing the JSON data if *fp* is `None`, otherwise doesn't \
    return anything. If ``validation='lax'`` keyword argument is provided the validation \
    errors are collected and returned, eventually coupled in a tuple with the JSON data.
    :raises: :exc:`XMLSchemaValidationError` if the object is not decodable by \
    the XSD component, or also if it's invalid when ``validation='strict'`` is provided.
    """
    kwargs.update(
        validation=validation,
        locations=locations,
        use_location_hints=use_location_hints
    )
    source, _schema = get_context(xml_document, schema, cls, **kwargs)
    if json_options is None:
        json_options = {}
    if 'decimal_type' not in kwargs:
        kwargs['decimal_type'] = float

    errors: list[XMLSchemaValidationError] = []

    if path is None and source.is_lazy() and 'cls' not in json_options:
        json_options['cls'] = get_lazy_json_encoder(errors)

    obj = _schema.decode(source, path=path, **kwargs)

    if isinstance(obj, tuple):
        errors.extend(obj[1])
        if fp is not None:
            json.dump(obj[0], fp, **json_options)
            return tuple(errors)
        else:
            result = json.dumps(obj[0], **json_options)
            return result, tuple(errors)
    elif fp is not None:
        json.dump(obj, fp, **json_options)
        return None if not errors else tuple(errors)
    else:
        result = json.dumps(obj, **json_options)
        return result if not errors else (result, tuple(errors))


def to_etree(obj: Any,
             schema: Optional[Union[XMLSchemaBase, SourceArgType]] = None,
             cls: Optional[type[XMLSchemaBase]] = None,
             path: Optional[str] = None,
             validation: str = 'strict',
             namespaces: Optional[NsmapType] = None,
             use_defaults: bool = True,
             converter: Optional[ConverterType] = None,
             unordered: bool = False,
             **kwargs: Any) -> EncodeType[ElementType]:
    """
    Encodes a data structure/object to an ElementTree's Element.

    :param obj: the Python object that has to be encoded to XML data.
    :param schema: can be a schema instance or a file-like object or a file path or a URL \
    of a resource or a string containing the schema. If not provided a dummy schema is used.
    :param cls: class to use for building the schema instance (for default uses \
    :class:`XMLSchema10`).
    :param path: is an optional XPath expression for selecting the element of the schema \
    that matches the data that has to be encoded. For default the first global element of \
    the schema is used.
    :param validation: the XSD validation mode. Can be 'strict', 'lax' or 'skip'.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    :param use_defaults: whether to use default values for filling missing data.
    :param converter: an :class:`XMLSchemaConverter` subclass or instance to use for \
    the encoding.
    :param unordered: a flag for explicitly activating unordered encoding mode for \
    content model data. This mode uses content models for a reordered-by-model \
    iteration of the child elements.
    :param kwargs: other optional arguments of :meth:`XMLSchemaBase.iter_encode` and \
    options for the converter.
    :return: An element tree's Element instance. If ``validation='lax'`` keyword argument is \
    provided the validation errors are collected and returned coupled in a tuple with the \
    Element instance.
    :raises: :exc:`XMLSchemaValidationError` if the object is not encodable by the schema, \
    or also if it's invalid when ``validation='strict'`` is provided.
    """
    if cls is None:
        cls = XMLSchema10
    elif not issubclass(cls, XMLSchemaBase):
        raise XMLSchemaTypeError("invalid schema class %r" % cls)

    if schema is None:
        if not path:
            raise XMLSchemaTypeError("without schema a path is required "
                                     "for building a dummy schema")

        if namespaces is None:
            tag = get_extended_qname(path, {'xsd': XSD_NAMESPACE, 'xs': XSD_NAMESPACE})
        else:
            tag = get_extended_qname(path, namespaces)

        if not tag.startswith('{') and ':' in tag:
            raise XMLSchemaTypeError("without schema the path must be "
                                     "mappable to a local or extended name")

        if tag == XSD_SCHEMA:
            assert cls.meta_schema is not None
            _schema = cls.meta_schema
        else:
            _schema = get_dummy_schema(tag, cls)

    elif isinstance(schema, XMLSchemaBase):
        _schema = schema
    else:
        _schema = cls(schema)

    return _schema.encode(
        obj=obj,
        path=path,
        validation=validation,
        namespaces=namespaces,
        use_defaults=use_defaults,
        converter=converter,
        unordered=unordered,
        **kwargs
    )


def from_json(source: Union[str, bytes, IO[str]],
              schema: Optional[Union[XMLSchemaBase, SourceArgType]] = None,
              cls: Optional[type[XMLSchemaBase]] = None,
              path: Optional[str] = None,
              validation: str = 'strict',
              namespaces: Optional[NsmapType] = None,
              use_defaults: bool = True,
              converter: Optional[ConverterType] = None,
              unordered: bool = False,
              json_options: Optional[dict[str, Any]] = None,
              **kwargs: Any) -> EncodeType[ElementType]:
    """
    Deserialize JSON data to an XML Element.

    :param source: can be a string or a :meth:`read()` supporting file-like object \
    containing the JSON document.
    :param schema: an :class:`XMLSchema10` or an :class:`XMLSchema11` instance.
    :param cls: class to use for building the schema instance (for default uses \
    :class:`XMLSchema10`).
    :param path: is an optional XPath expression for selecting the element of the schema \
    that matches the data that has to be encoded. For default the first global element of \
    the schema is used.
    :param validation: the XSD validation mode. Can be 'strict', 'lax' or 'skip'.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    :param use_defaults: whether to use default values for filling missing data.
    :param converter: an :class:`XMLSchemaConverter` subclass or instance to use for \
    the encoding.
    :param unordered: a flag for explicitly activating unordered encoding mode for \
    content model data. This mode uses content models for a reordered-by-model \
    iteration of the child elements.
    :param json_options: a dictionary with options for the JSON deserializer.
    :param kwargs: other optional arguments of :meth:`XMLSchemaBase.iter_encode` and \
    options for converter.
    :return: An element tree's Element instance. If ``validation='lax'`` keyword argument is \
    provided the validation errors are collected and returned coupled in a tuple with the \
    Element instance.
    :raises: :exc:`XMLSchemaValidationError` if the object is not encodable by the schema, \
    or also if it's invalid when ``validation='strict'`` is provided.
    """
    if json_options is None:
        json_options = {}

    if isinstance(source, (str, bytes)):
        obj = json.loads(source, **json_options)
    else:
        obj = json.load(source, **json_options)

    return to_etree(
        obj=obj,
        schema=schema,
        cls=cls,
        path=path,
        validation=validation,
        namespaces=namespaces,
        use_defaults=use_defaults,
        converter=converter,
        unordered=unordered,
        **kwargs
    )


class XmlDocument(XMLResource):
    """
    An XML document bound with its schema. If no schema is get from the provided
    context and validation argument is 'skip' the XML document is associated with
    a generic schema, otherwise a ValueError is raised.

    :param source: a string containing XML data or a file path or a URL or a \
    file like object or an ElementTree or an Element.
    :param schema: can be a :class:`xmlschema.XMLSchema` instance or a file-like \
    object or a file path or a URL of a resource or a string containing the XSD schema.
    :param cls: class to use for building the schema instance (for default \
    :class:`XMLSchema10` is used).
    :param validation: the XSD validation mode to use for validating the XML document, \
    that can be 'strict' (default), 'lax' or 'skip'.
    :param namespaces: is an optional mapping from namespace prefix to URI.
    :param locations: resource location hints, that can be a dictionary or a \
    sequence of couples (namespace URI, resource URL).
    :param use_location_hints: for default, in case a schema instance has \
    to be built, uses also schema locations hints provided within XML data. \
    set this option to `False` to ignore these schema location hints.
    :param kwargs: other optional arguments for building :class:`XMLResource` or \
    :class:`XMLSchema` instances provided as keyword arguments.
    """
    errors: Union[tuple[()], list[XMLSchemaValidationError]] = ()

    # Additional arguments
    schema: SchemaArgument = SchemaArgument()
    _schema: SchemaType
    validation: ValidationOption = ValidationOption(default='strict')
    locations: LocationsOption = LocationsOption(default=None)
    use_location_hints: BooleanOption = BooleanOption(default=True)

    def __init__(self, source: XMLSourceType,
                 schema: Optional[Union[XMLSchemaBase, SourceArgType]] = None,
                 cls: Optional[type[XMLSchemaBase]] = None,
                 validation: str = 'strict',
                 namespaces: Optional[NsmapType] = None,
                 locations: Optional[LocationsType] = None,
                 use_location_hints: bool = True,
                 **kwargs: Any) -> None:

        super().__init__(source, **{k: kwargs[k] for k in kwargs if k in RESOURCE_KWARGS})

        self.validation = validation
        self.use_location_hints = use_location_hints
        self._init_namespaces = get_namespace_map(namespaces)
        self.namespaces = super().get_namespaces(namespaces, root_only=True)
        self.schema = get_resource_schema(
            resource=self,
            schema=schema,
            cls=cls,
            validation=validation,
            locations=locations,
            use_location_hints=use_location_hints,
            **{k: kwargs[k] for k in kwargs if k in SCHEMA_KWARGS}
        )

        if validation == 'strict':
            self._schema.validate(self, namespaces=self.namespaces)
        elif validation == 'lax':
            self.errors = [e for e in self._schema.iter_errors(self, namespaces=self.namespaces)]
        elif validation != 'skip':
            raise XMLSchemaValueError("%r is not a validation mode" % validation)

    def get_arguments(self) -> dict[str, Any]:
        """Returns keyword arguments for rebuilding the XML document."""
        kwargs = super().get_arguments()
        kwargs.update(
            validation=self.validation,
            schema=self.schema,
            namespaces=self._init_namespaces
        )
        return kwargs

    def get_namespaces(self, namespaces: Optional[NsmapType] = None,
                       root_only: bool = True, root_default: bool = False) -> dict[str, str]:
        namespaces = get_namespace_map(namespaces)
        update_namespaces(namespaces, self.namespaces.items(), root_declarations=True)
        return super().get_namespaces(namespaces, root_only, root_default)

    def getroot(self) -> ElementType:
        """Get the root element of the XML document."""
        return self.root

    def get_etree_document(self) -> Any:
        """
        The resource as ElementTree XML document. If the resource is lazy
        raises a resource error.
        """
        if is_etree_document(self._source):
            return self._source
        elif self._lazy:
            raise XMLResourceError(
                "cannot create an ElementTree instance from a lazy XML resource"
            )
        elif hasattr(self.root, 'getroottree'):
            return self.root.getroottree()
        else:
            return ElementTree.ElementTree(self.root)

    def decode(self, **kwargs: Any) -> DecodeType[Any]:
        """
        Decode the XML document to a nested Python dictionary.

        :param kwargs: options for the decode/to_dict method of the schema instance.
        """
        if 'validation' not in kwargs:
            kwargs['validation'] = self.validation
        if 'namespaces' not in kwargs:
            kwargs['namespaces'] = self.namespaces

        obj = self._schema.to_dict(self, **kwargs)
        return obj[0] if isinstance(obj, tuple) else obj

    def to_json(self, fp: Optional[IO[str]] = None,
                json_options: Optional[dict[str, Any]] = None,
                **kwargs: Any) -> JsonDecodeType:
        """
        Converts loaded XML data to a JSON string or file.

        :param fp: can be a :meth:`write()` supporting file-like object.
        :param json_options: a dictionary with options for the JSON deserializer.
        :param kwargs: options for the decode/to_dict method of the schema instance.
        """
        if json_options is None:
            json_options = {}
        path = kwargs.pop('path', None)
        if 'validation' not in kwargs:
            kwargs['validation'] = self.validation
        if 'namespaces' not in kwargs:
            kwargs['namespaces'] = self.namespaces
        if 'decimal_type' not in kwargs:
            kwargs['decimal_type'] = float

        errors: list[XMLSchemaValidationError] = []

        if path is None and self._lazy and 'cls' not in json_options:
            json_options['cls'] = get_lazy_json_encoder(errors)
            kwargs['lazy_decode'] = True

        obj = self._schema.decode(self, path=path, **kwargs)
        if isinstance(obj, tuple):
            if fp is not None:
                json.dump(obj[0], fp, **json_options)
                obj[1].extend(errors)
                return tuple(obj[1])
            else:
                result = json.dumps(obj[0], **json_options)
                obj[1].extend(errors)
                return result, tuple(obj[1])

        elif fp is not None:
            json.dump(obj, fp, **json_options)
            return None if not errors else tuple(errors)
        else:
            result = json.dumps(obj, **json_options)
            return result if not errors else (result, tuple(errors))

    def write(self, file: Union[str, TextIO, BinaryIO],
              encoding: str = 'us-ascii', xml_declaration: bool = False,
              default_namespace: Optional[str] = None, method: str = "xml") -> None:
        """Serialize an XML resource to a file. Cannot be used with lazy resources."""
        if self._lazy:
            raise XMLResourceError("cannot serialize a lazy XML resource")

        kwargs: dict[str, Any] = {
            'xml_declaration': xml_declaration,
            'encoding': encoding,
            'method': method,
        }
        if not default_namespace:
            kwargs['namespaces'] = self.namespaces
        else:
            namespaces: Optional[dict[Optional[str], str]]
            namespaces = {k: v for k, v in self.namespaces.items()}

            if hasattr(self.root, 'nsmap'):
                # noinspection PyTypeChecker
                namespaces[None] = default_namespace
            else:
                namespaces[''] = default_namespace
            kwargs['namespaces'] = namespaces

        _string = etree_tostring(self.root, **kwargs)

        match file:
            case str():
                if isinstance(_string, str):
                    with open(file, 'w', encoding='utf-8') as fp:
                        fp.write(_string)
                else:
                    with open(file, 'wb') as _fp:
                        _fp.write(_string)

            case TextIOBase():
                if isinstance(_string, bytes):
                    file.write(_string.decode('utf-8'))
                else:
                    file.write(_string)

            case IOBase():
                if isinstance(_string, str):
                    file.write(_string.encode('utf-8'))
                else:
                    file.write(_string)
            case _:
                msg = "unexpected type %r for 'file' argument"
                raise XMLSchemaTypeError(msg % type(file))
