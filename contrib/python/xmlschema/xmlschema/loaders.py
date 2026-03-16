#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import logging
import warnings
import functools
from importlib.resources.abc import Traversable
from operator import attrgetter
from types import MappingProxyType
from typing import Any, Optional, TYPE_CHECKING, cast

from xmlschema.aliases import SchemaType, SourceArgType, LocationsType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError, \
    XMLResourceBlocked, XMLResourceForbidden, XMLResourceError, XMLResourceParseError
from xmlschema.translation import gettext as _
from xmlschema.utils.urls import normalize_url
from xmlschema.utils.etree import iter_schema_declarations
from xmlschema.arguments import Option, validate_subclass
from xmlschema.locations import NamespaceResourcesMap, get_locations, \
    LOCATIONS, FALLBACK_LOCATIONS

import xmlschema.names as nm

from xmlschema.validators import GlobalMaps, XMLSchemaParseError, XMLSchemaIncludeWarning, \
    XMLSchemaImportWarning

if TYPE_CHECKING:
    from xmlschema.validators import XsdGlobals  # noqa:F401

logger = logging.getLogger('xmlschema')
base_url_attribute = attrgetter('name')


class SchemaLoader:
    """
    The default schema loader, that processes an import statement only
    if the referred namespace is not imported yet.
    """
    fallback_locations = MappingProxyType({**LOCATIONS, **FALLBACK_LOCATIONS})

    locations: NamespaceResourcesMap[str]
    schema_class: type[SchemaType]
    missing_locations: set[str]  # Missing or failing resource locations

    __slots__ = ('maps', 'namespaces', 'locations', 'missing_locations', '__dict__')

    def __init__(self,
                 maps: 'XsdGlobals',
                 locations: Optional[LocationsType] = None,
                 use_fallback: bool = True) -> None:

        self.maps = maps
        self.namespaces = maps.namespaces
        self.schema_class = type(maps.validator)
        self.locations = get_locations(locations, maps.validator.base_url)
        self.missing_locations = set()

        if not use_fallback:
            self.fallback_locations = MappingProxyType({})

    def clear(self) -> None:
        self.maps.clear()
        self.missing_locations.clear()

    def get_locations(self, namespace: str, location: Optional[str] = None) -> list[str]:
        locations: list[str] = [] if location is None else [location]
        if namespace in self.locations:
            locations.extend(self.locations[namespace])

        if namespace in self.fallback_locations:
            values = self.fallback_locations[namespace]
            if isinstance(values, (str, Traversable)):
                locations.append(values)
            else:
                locations.extend(values)

        return locations

    def load_declared_schemas(self, schema: SchemaType,
                              other_sources: Optional[list[SourceArgType]] = None) -> None:
        """
        Processes xs:include, xs:redefine, xs:override and xs:import statements,
        loading the schemas and/or the namespaced referred into declarations.
        """
        if schema not in self.maps.schemas or schema.maps is not self.maps:
            raise XMLSchemaValueError(f"{schema} is not owned by {self.maps}!")

        logger.debug("Processes inclusions and imports of schema %r", self)
        schema.imported_namespaces.clear()
        base_url = schema.base_url

        for elem in iter_schema_declarations(schema.source.root):
            location = elem.get('schemaLocation')
            if elem.tag == nm.XSD_IMPORT:
                namespace = elem.get('namespace', '').strip()
                if namespace == schema.target_namespace:
                    if namespace == '':
                        msg = _("if the 'namespace' attribute is not present on "
                                "the import statement then the imported schema "
                                "must have a 'targetNamespace'")
                    else:
                        msg = _("the attribute 'namespace' must be different "
                                "from schema's 'targetNamespace'")
                    schema.parse_error(msg)
                    continue

                schema.imported_namespaces.append(namespace)
                if isinstance(base_url, Traversable) and location:
                    url = base_url / location
                elif isinstance(base_url, Traversable) and not location:
                    url = None
                else:
                    url = normalize_url(location, base_url) if location else None
                if self.is_missing(namespace, location, base_url):
                    self.import_namespace(schema, namespace, url)

            elif location is not None:
                # Process xs:include/xs:redefine/xs:override only if it has a
                # schemaLocation attribute. If not an error has been already
                # generated by the XSD validation with against the meta-schema.

                operation = elem.tag.split('}')[-1]
                logger.info("Process xs:%s schema from %s", operation, location)
                try:
                    _schema = self.include_schema(schema, location, base_url)
                except OSError as err:
                    # It is not an error if the location fails to resolve:
                    #   https://www.w3.org/TR/2012/REC-xmlschema11-1-20120405/#compound-schema
                    #   https://www.w3.org/TR/2012/REC-xmlschema11-1-20120405/#src-include
                    self.missing_locations.add(location)
                    if elem.tag == nm.XSD_INCLUDE:
                        msg = _("Include schema failed: {}").format(err)
                    else:
                        # If the xs:redefine/xs:override doesn't contain components (annotation
                        # excluded) the statement is equivalent to an include, so no error is
                        # generated, otherwise fails.
                        if any(e.tag != nm.XSD_ANNOTATION and not callable(e.tag) for e in elem):
                            schema.parse_error(str(err), elem)

                        if elem.tag == nm.XSD_REDEFINE:
                            msg = _("Redefine schema failed: {}").format(err)
                        else:
                            msg = _("Override schema failed: {}").format(err)

                    schema.warnings.append(msg)
                    warnings.warn(msg, XMLSchemaIncludeWarning, stacklevel=3)

                except (XMLSchemaParseError, XMLSchemaTypeError, XMLResourceParseError) as err:
                    if elem.tag == nm.XSD_INCLUDE:
                        msg = _("can't include schema {!r}: {}")
                    elif elem.tag == nm.XSD_REDEFINE:
                        msg = _("can't redefine schema {!r}: {}")
                    else:
                        msg = _("can't override schema {!r}: {}")

                    if isinstance(err, (XMLSchemaParseError, XMLResourceParseError)):
                        schema.parse_error(msg.format(location, err), elem)
                    else:
                        raise type(err)(msg.format(location, err))
                else:
                    if _schema is schema and operation != 'include':
                        msg = _("can't {} the same schema {!r}").format(operation, schema)
                        schema.parse_error(msg, elem)
                    elif operation == 'redefine':
                        _schema.redefine = schema
                    elif operation == 'override':
                        _schema.override = schema

        logger.debug("Inclusions and imports of schema %r processed", schema)

        # Import other namespaces provided by argument (usually from xsi:schemaLocation attribute).
        if self.maps.validator is schema:
            for ns in self.locations:
                if ns not in self.maps.namespaces:
                    self.import_namespace(schema, ns)

        # Add explicitly provided other schemas
        if other_sources:
            for other in other_sources:
                schema.add_schema(other, base_url=base_url)

    def import_namespace(self, schema: SchemaType,
                         namespace: str,
                         location: Optional[str] = None) -> None:
        import_error: Optional[Exception] = None
        locations = self.get_locations(namespace, location)

        for url in locations:
            try:
                logger.info("Import namespace %r from %r", namespace, url)
                self.import_schema(schema, namespace, url, schema.base_url)
            except (OSError, XMLResourceBlocked, XMLResourceForbidden) as err:
                # It's not an error if the location access fails (ref. section 4.2.6.2):
                #   https://www.w3.org/TR/2012/REC-xmlschema11-1-20120405/#composition-schemaImport
                #
                # Also consider block or defuse of XML data as a location access fail.
                self.missing_locations.add(url)
                logger.debug('%s', err)
                if import_error is None:
                    import_error = err
            except (XMLSchemaParseError, XMLSchemaTypeError, XMLResourceParseError) as err:
                if namespace:
                    msg = _("import of namespace {!r} failed: {}").format(namespace, err)
                else:
                    msg = _("import of chameleon schema failed: {}").format(err)

                if isinstance(err, (XMLSchemaParseError, XMLResourceParseError)):
                    schema.parse_error(msg)
                else:
                    raise type(err)(msg)

            except XMLSchemaValueError as err:
                schema.parse_error(err)
            else:
                logger.info("Namespace %r imported from %r", namespace, url)
                break
        else:
            if import_error is not None:
                msg = _("Import of namespace {!r} from {!r} failed: {}.")
                schema.warnings.append(msg.format(namespace, locations, str(import_error)))
                warnings.warn(schema.warnings[-1], XMLSchemaImportWarning, stacklevel=4)

    def import_schema(self, target_schema: SchemaType,
                      namespace: str,
                      location: str,
                      base_url: Optional[str] = None,
                      build: bool = False,
                      partial: bool = False) -> Optional[SchemaType]:
        """
        Imports a schema from a specific URL.

        :param target_schema: the importer schema.
        :param namespace: is the URI of the external namespace.
        :param location: is the URL of the schema.
        :param base_url: is an optional base URL for fetching the schema resource, \
        otherwise use the base url of the loader, if any.
        :param build: defines when to build the imported schema, the default is to not build.
        :param partial: if `True`, the loaded schema is initialized without processing \
        imports/inclusions and the build phase is skipped.
        :return: the imported :class:`XMLSchema` instance.
        """
        if location in target_schema.imports:
            schema = target_schema.imports[location]
            if schema is not None and schema.target_namespace != namespace:
                msg = _('imported schema {!r} has an unmatched namespace {!r}')
                raise XMLSchemaValueError(msg.format(location, namespace))

        if namespace in self.namespaces:
            logger.debug("Import schema in namespace %r from %r", namespace, location)
        else:
            logger.debug("Import namespace %r from %r", namespace, location)

        schema = self.load_schema(location, namespace, base_url, build, partial)
        if target_schema is schema:
            return schema
        elif location not in target_schema.imports:
            target_schema.imports[location] = schema

        return schema

    def include_schema(self, target_schema: SchemaType,
                       location: str,
                       base_url: Optional[str] = None,
                       build: bool = False,
                       partial: bool = False) -> SchemaType:
        """
        Includes a schema for the same namespace from a specific location.

        :param target_schema: the schema that includes the schema at provided location.
        :param location: is the URL of the schema.
        :param base_url: is an optional base URL for fetching the schema resource.
        :param build: defines when to build the imported schema, the default is to not build.
        :return: the included :class:`XMLSchema` instance.
        :param partial: if `True`, the included schema is initialized without processing \
        imports/inclusions and the build phase is skipped.
        :return: the included :class:`XMLSchema` instance.
        """
        if location in target_schema.includes:
            return target_schema.includes[location]

        namespace = target_schema.target_namespace
        logger.debug("Include schema in namespace %r from %r", namespace, location)
        schema = self.load_schema(location, namespace, base_url, build, partial)

        if target_schema is not schema:
            target_schema.includes[location] = schema
        return schema

    def load_schema(self, source: SourceArgType,
                    namespace: Optional[str] = None,
                    base_url: Optional[str] = None,
                    build: bool = False,
                    partial: bool = False) -> SchemaType:
        """
        Loads a schema from a location.

        :param source: a URI that reference to a resource or a file path or a file-like \
        object or a string containing the schema or an Element or an ElementTree document.
        :param namespace: is the URI of the namespace that the schema belongs to.
        :param base_url: is an optional base URL for fetching the schema resource, \
        otherwise use the base url of the loader, if any.
        :param build: defines when to build the loaded schema, the default is to not build.
        :param partial: if `True`, the loaded schema is initialized without processing \
        imports/inclusions and the build phase is skipped.
        :return: the loaded schema or the schema that matches the URL if it's already loaded.
        """
        schema = self.maps.get_schema(namespace, source, base_url)
        if schema is not None:
            logger.info("Resource %r is already loaded", schema.source)
            return schema

        return self.schema_class(
            source=source,
            namespace=namespace,
            validation=self.maps.validation,
            global_maps=self.maps,
            base_url=base_url or self.maps.settings.base_url,
            build=build,
            partial=partial,
        )

    def load_namespace(self, namespace: str, build: bool = True) -> bool:
        """
        Load namespace from available location hints. Returns `True` if the namespace
        is already loaded or if the namespace can be loaded from one of the locations,
        returns `False` otherwise. Failing locations are inserted into the missing
        locations list.

        :param namespace: the namespace to load.
        :param build: if left with `True` value builds the maps after load. If the \
        build fails the resource URL is added to missing locations and the global \
        maps are restored at previous state.
        """
        if namespace in self.maps.namespaces:
            return True
        elif self.maps.validator.meta_schema is None:
            return True  # Do not load additional namespaces in meta-schema maps

        if not build:
            for url in self.get_locations(namespace):
                if self.is_missing(namespace, url):
                    try:
                        self.load_schema(url, namespace)
                    except OSError:
                        self.missing_locations.add(url)
            return namespace in self.namespaces

        with self.maps.protect_status(reraise=False):
            for url in self.get_locations(namespace):
                if self.is_missing(namespace, url):
                    try:
                        self.load_schema(url, namespace)
                    except OSError:
                        self.missing_locations.add(url)

            self.maps.build()

        return namespace in self.namespaces

    def is_missing(self, namespace: str, location: Optional[str] = None,
                   base_url: Optional[str] = None) -> bool:
        """Returns `True` if the given location hint is missing, `False` otherwise."""
        return namespace not in self.namespaces or \
            all(s.maps is not self.maps for s in self.namespaces[namespace])


class LocationSchemaLoader(SchemaLoader):
    """
    A schema loader that processes an import statement if the
    referred location is not already loaded.
    """
    def is_missing(self, namespace: str, location: Optional[str] = None,
                   base_url: Optional[str] = None) -> bool:
        if super().is_missing(namespace):
            return True
        elif location is None:
            return False
        else:
            url = normalize_url(location, base_url)
            return all(not s.source.match_location(url) for s in self.maps.schemas)


class SafeSchemaLoader(SchemaLoader):
    """
    A schema loader that processes an import statement if the
    referred location is not already loaded and after checking
    if there aren't collisions with loaded schemas.
    """
    def __init__(self, maps: 'XsdGlobals', *args: Any, **kwargs: Any) -> None:
        super().__init__(maps, *args, **kwargs)
        self.global_maps = GlobalMaps.from_builders(maps.validator.builders)
        self.excluded_locations: set[str] = set()

    def clear(self) -> None:
        self.maps.clear()
        self.missing_locations.clear()
        self.global_maps.clear()
        self.excluded_locations.clear()

    def is_missing(self, namespace: str, location: Optional[str] = None,
                   base_url: Optional[str] = None) -> bool:
        if super().is_missing(namespace):
            return True
        elif location is None:
            return False

        url = normalize_url(location, base_url)
        if url in self.excluded_locations or \
                any(s.source.match_location(url) for s in self.maps.schemas):
            return False

        schema = self.maps.get_schema(namespace, url)
        if schema is not None and schema.maps is self.maps:
            return False

        try:
            schema = self.load_schema(location, namespace, base_url, partial=True)
        except (OSError, XMLResourceError):
            self.missing_locations.add(url)
            return False  # The resource is not accessible

        self.global_maps.clear()
        try:
            self.global_maps.load(self.namespaces[namespace])
        except XMLSchemaParseError:
            self.namespaces[namespace].pop()
            self.maps.schemas.remove(schema)
            self.excluded_locations.add(url)
            return False
        else:
            self.namespaces[namespace].pop()
            self.maps.schemas.remove(schema)
            return True


class LoaderClassOption(Option[type[SchemaLoader]]):
    _validators = functools.partial(validate_subclass, cls=SchemaLoader, none=True),

    def validated_value(self, value: Any) -> type[SchemaLoader]:
        if value is None:
            return self._default
        return super().validated_value(value)

        return cast(type[SchemaLoader], value)
