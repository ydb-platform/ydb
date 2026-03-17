#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from dataclasses import asdict, dataclass, replace, fields
from typing import cast, Optional, Any, Union
from xml.etree.ElementTree import Element

from elementpath.datatypes import AnyAtomicType

from xmlschema.aliases import SettingsType, BaseUrlType, DecodedValueType, \
    GlobalMapsType, SourceArgType, SchemaType, XMLSourceType
from xmlschema.exceptions import XMLSchemaTypeError, XMLResourceError, XMLSchemaValueError
from xmlschema.translation import gettext as _
from xmlschema.arguments import BooleanOption, BaseUrlOption, AllowOption, \
    DefuseOption, LazyOption, BlockOption, UriMapperOption, IterParseOption, \
    SelectorOption, OpenerOption, PositiveIntOption, LocationsOption, \
    ValidationOption, LogLevelOption
from xmlschema.utils.decoding import raw_encode_value, raw_encode_attributes
from xmlschema.utils.etree import is_etree_element, is_etree_document
from xmlschema.resources import XMLResource
from xmlschema.converters import XMLSchemaConverter, ConverterOption, ConverterType
from xmlschema.loaders import SchemaLoader, LoaderClassOption
from xmlschema.caching import SchemaCache
from xmlschema.xpath import ElementSelector


@dataclass
class ResourceSettings:
    """Settings for accessing XML resources."""

    base_url: BaseUrlOption = BaseUrlOption(default=None)
    """
    An optional base URL, used for the normalization of relative paths when the URL
    of the XML resource can't be obtained from the source argument.
    """

    allow: AllowOption = AllowOption(default='all')
    """
    The security mode for accessing resource locations. Can be 'all', 'remote',
    'local' or 'sandbox'. Default is 'all' that means all types of URLs are allowed.
    With 'remote' only remote resource URLs are allowed. With 'local' only file paths
    and URLs are allowed. With 'sandbox' only file paths and URLs that are under the
    directory path identified by source or by the *base_url* argument are allowed.
    """

    defuse: DefuseOption = DefuseOption(default='remote')
    """
    Defines when to defuse XML data using a `SafeXMLParser`. Can be 'always',
    'remote' or 'never'. For default defuses only remote XML data.
    """

    timeout: PositiveIntOption = PositiveIntOption(default=300)
    """The timeout in seconds for accessing remote resources. Default is `300` seconds."""

    lazy: LazyOption = LazyOption(default=False)
    """
    Defines if the XML data is fully loaded and processed in memory, that is for default.
    Setting `True` or a positive integer only the root element of the source is loaded
    when the XMLResource instance is created. The root and the other parts are reloaded
    at each iteration, pruning the processed subtrees at the depth defined by this option
    (`True` means 1).
    """

    thin_lazy: BooleanOption = BooleanOption(default=True)
    """
    For default, in order to reduce the memory usage, during the iteration of a lazy
    resource deletes also the preceding elements after the use. Setting `False` only
    descendant elements are deleted at the depth defined by *lazy* option.
    """

    block: BlockOption = BlockOption(default=None)
    """
    Defines which types of sources are blocked for security reasons. For default none
    of possible types are blocked. Set with a space separated string of words, choosing
    between 'text', 'file', 'io', 'url' and 'tree' or a tuple/list of them to select
    which types are blocked.
    """

    uri_mapper: UriMapperOption = UriMapperOption(default=None)
    """
    Optional URI mapper for using relocated or URN-addressed resources. Can be a
    dictionary or a function that takes the URI string and returns a URL, or the
    argument if there is no mapping for it.
    """

    opener: OpenerOption = OpenerOption(default=None)
    """
    Optional :class:`OpenerDirector` to use for open XML resources.
    For default the opener installed globally for *urlopen* is used.
    """

    iterparse: IterParseOption = IterParseOption(default=None)
    """
    Optional callable that returns an iterator parser used for building the
    XML trees. For default *ElementTree.iterparse* is used. XSD schemas are
    built using only *ElementTree.iterparse*, because *lxml* is unsuitable
    for multitree structures and for pruning.
    """

    selector: SelectorOption = SelectorOption(default=ElementSelector)
    """The selector class to use for XPath element selectors."""

    _DEFAULT_SETTINGS = '_DEFAULT_RESOURCE_SETTINGS'

    @classmethod
    def get_settings(cls, **kwargs: Any) -> 'ResourceSettings':
        """Returns settings from defaults, applying provided overrides."""
        return cast(ResourceSettings, replace(globals()[cls._DEFAULT_SETTINGS], **kwargs))

    @classmethod
    def get_defaults(cls) -> SettingsType:
        """Returns the current default settings for XML resources."""
        return cast(ResourceSettings, globals()[cls._DEFAULT_SETTINGS])

    @classmethod
    def update_defaults(cls, **kwargs: Any) -> None:
        """Overrides the default settings for schemas."""
        globals()[cls._DEFAULT_SETTINGS] = cls.get_settings(**kwargs)

    @classmethod
    def reset_defaults(cls) -> None:
        """Resets the default settings for to initial values."""
        globals()[cls._DEFAULT_SETTINGS] = cls()

    def get_resource(self, cls: type[XMLResource],
                     source: XMLSourceType,
                     **kwargs: Any) -> XMLResource:
        """
        Returns a :class:`xmlschema.XMLResource` instance from settings, overriding
        defaults with provided keyword arguments.
        """
        options = {fld.name: getattr(self, fld.name) for fld in fields(ResourceSettings)}
        return cls(source, **{**options, **kwargs})


@dataclass
class SchemaSettings(ResourceSettings):
    """
    Settings for schemas. A :class:`xmlschema.settings.SchemaSettings` object
    includes settings for XML resources.
    """

    validation: ValidationOption = ValidationOption(default='strict')
    """
    The XSD validation mode to use for build the schema. Can be 'strict', 'lax' or 'skip'.
    """

    converter: ConverterOption = ConverterOption(default=None)
    """The converter to use for decoding/encoding XML data."""

    locations: LocationsOption = LocationsOption(default=None)
    """Optional schema extra location hints with additional namespaces to import."""

    use_location_hints: BooleanOption = BooleanOption(default=False)
    """
    Schema locations hints provided within XML data for dynamic schema loading.
    For default these hints are ignored by schemas in order to avoid the change of
    schema instance. Set this option to `True` to activate dynamic schema loading.
    """

    loader_class: LoaderClassOption = LoaderClassOption(default=SchemaLoader)
    """
    An optional subclass of :class:`SchemaLoader` to use for creating the loader instance.
    """

    use_fallback: BooleanOption = BooleanOption(default=True)
    """
    If `True` the schema processor uses the validator fallback location hints
    to load well-known namespaces (e.g. xhtml).
    """

    use_xpath3: BooleanOption = BooleanOption(default=False)
    """
    If `True` an XSD 1.1 schema instance uses the XPath 3 processor for assertions.
    For default a full XPath 2.0 processor is used.
    """

    use_meta: BooleanOption = BooleanOption(default=True)
    """
    If `True` the schema processor uses the validator meta-schema as parent schema.
    Ignored if either *global_maps* or *parent* argument is provided.
    """

    use_cache: BooleanOption = BooleanOption(default=True)
    """
    If `True` the schemas processor creates a :class:`SchemaCache` for caching several
    method calls component instances. For default the cache is enabled except for
    predefined meta-schemas.
    """

    loglevel: LogLevelOption = LogLevelOption(default=None)
    """
    Used for setting a different logging level for schema initialization and building.
    For default is the logging level is set to WARNING (30). For INFO level set it
    with 20, for DEBUG level with 10. The default loglevel is restored after schema
    building, when exiting the initialization method.
    """

    _DEFAULT_SETTINGS = '_DEFAULT_SCHEMA_SETTINGS'

    def get_xml_resource(self, source: SourceArgType) -> XMLResource:
        """
        Returns a :class:`xmlschema.XMLResource` instance for the given XML source
        using schema settings.
        """
        if isinstance(source, XMLResource):
            return source

        return XMLResource(
            source=source,
            base_url=self.base_url,
            allow=self.allow,
            defuse=self.defuse,
            timeout=self.timeout,
            lazy=self.lazy,
            thin_lazy=self.thin_lazy,
            block=self.block,
            uri_mapper=self.uri_mapper,
            opener=self.opener,
            iterparse=self.iterparse,
            selector=self.selector,
        )

    def get_resource_from_data(self, source: Any, tag: Optional[str] = None) -> XMLResource:
        """
        Returns a :class:`xmlschema.XMLResource` instance from XML data. Build a dummy
        Element if the source is a dictionary or an atomic value. Do not load
        XML data from locations or local streams.

        :param source: XML source data.
        :param tag: XML tag to use for building the dummy element, if necessary.
        """
        if isinstance(source, XMLResource):
            if source.is_lazy():
                msg = _("component validation/decoding doesn't support lazy mode")
                raise XMLResourceError(msg)
            return source
        elif is_etree_element(source) or is_etree_document(source):
            return self.get_xml_resource(source)
        elif isinstance(source, dict):
            attrib = raw_encode_attributes(source)
            root = Element(tag or 'root', attrib=attrib)
            return self.get_xml_resource(root)
        elif source is None or isinstance(source, (AnyAtomicType, bytes)):
            root = Element(tag or 'root')
            root.text = raw_encode_value(cast(DecodedValueType, source))
            return self.get_xml_resource(root)
        else:
            msg = _("incompatible source type {!r}")
            raise TypeError(msg.format(type(source)))

    def get_schema_resource(self, source: SourceArgType,
                            base_url: Optional[BaseUrlType] = None) -> XMLResource:
        """
        Returns a :class:`xmlschema.XMLResource` instance suitable for building schemas.
        Use only ElementTree library and fully loaded resources. The `lxml.etree`
        library cannot be used because components definitions sometimes require
        the build of additional elements that share a child.
        """
        if isinstance(source, XMLResource):
            if source.is_lazy():
                msg = _("schemas don't support lazy mode")
                raise XMLResourceError(msg)
            elif 'lxml' in source.iterparse.__module__:
                msg = _("schemas can't be built using lxml.etree library")
                raise XMLResourceError(msg)
            return source

        return XMLResource(
            source=source,
            base_url=base_url or self.base_url,
            allow=self.allow,
            defuse=self.defuse,
            timeout=self.timeout,
            block=self.block,
            uri_mapper=self.uri_mapper,
            opener=self.opener,
        )

    def get_converter(self, converter: Optional[ConverterType] = None,
                      **kwargs: Any) -> XMLSchemaConverter:
        """
        Returns a new converter instance, with a fallback to the optional converter
        saved with the settings.

        :param converter: can be a converter class or instance. If not provided the \
        converter option of the schema settings is used.
        :param kwargs: optional arguments to initialize the converter instance.
        :return: a converter instance.
        """
        if converter is None:
            converter = self.converter

        if converter is None:
            return XMLSchemaConverter(**kwargs)
        elif isinstance(converter, XMLSchemaConverter):
            return converter.replace(**kwargs)
        elif isinstance(converter, type) and issubclass(converter, XMLSchemaConverter):
            return converter(**kwargs)  # noqa
        else:
            msg = _("'converter' argument must be a {0!r} subclass or instance: {1!r}")
            raise XMLSchemaTypeError(msg.format(XMLSchemaConverter, converter))

    def get_loader(self, maps: GlobalMapsType) -> SchemaLoader:
        """Returns a new :class:`SchemaLoader` instance for the given maps."""
        return self.loader_class(
            maps=maps,
            locations=self.locations,
            use_fallback=self.use_fallback
        )

    def get_cache(self) -> SchemaCache:
        """Returns a new :class:`SchemaCache` instance for schema settings."""
        return SchemaCache(self.use_cache)

    def get_schema(self, cls: type[SchemaType],
                   source: Union[SourceArgType, list[SourceArgType]],
                   **kwargs: Any) -> SchemaType:
        """
        Returns a new schema instance from schema settings. Optional keyword arguments
        must be options for schema initialization and can be passed also to override
        some settings. If a `global_map` argument is provided, it will be removed and
        used to provide a `parent` argument.

        :param cls: schema class.
        :param source: the schema source.
        :param kwargs: optional arguments to initialize the schema instance.
        """
        maps: Optional[GlobalMapsType] = kwargs.pop('maps', None)
        if maps is not None:
            if kwargs.get('parent') is not None:
                msg = _("'global_maps' and 'parent' arguments are mutually exclusive")
                raise XMLSchemaValueError(msg)
            kwargs['parent'] = maps.validator

        return cls(source, **{**asdict(self), **kwargs})


# Default package settings for resources and schemas
_DEFAULT_RESOURCE_SETTINGS = ResourceSettings()
_DEFAULT_SCHEMA_SETTINGS = SchemaSettings()
