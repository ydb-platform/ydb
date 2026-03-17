#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import re
import logging
import pprint
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from collections.abc import Iterable
from typing import Any, Optional, Union
from urllib.parse import unquote, urlsplit
from xml.etree import ElementTree

from xmlschema.aliases import SchemaType
from xmlschema.exceptions import XMLSchemaValueError, XMLResourceOSError
from xmlschema.names import XSD_SCHEMA, XSD_IMPORT, XSD_INCLUDE, XSD_REDEFINE, XSD_OVERRIDE
from xmlschema.utils.logger import logged
from xmlschema.utils.paths import LocationPath
from xmlschema.utils.urls import is_remote_url, normalize_url, match_location
from xmlschema.translation import gettext as _
from xmlschema.resources import XMLResource

logger = logging.getLogger('xmlschema')

FIND_PATTERN = r'\bschemaLocation\s*=\s*[\'"]([^\'"]*)[\'"]'
REPLACE_PATTERN = r'\bschemaLocation\s*=\s*[\'"]\s*{0}\s*[\'"]'


@dataclass
class XsdSource:
    """Class for keeping track of an XSD schema source."""
    path: LocationPath
    resource: XMLResource

    def __init__(self, path: LocationPath, resource: XMLResource) -> None:
        self.path = path
        self.resource = resource
        self.text = resource.get_text()
        self.processed = False
        self.modified = False
        self.substitutions: Optional[list[tuple[str, str]]] = None

    @property
    def schema_locations(self) -> set[str]:
        """Extract schema locations from XSD resource tree."""
        locations = set()
        for child in self.resource.root:
            if child.tag in (XSD_IMPORT, XSD_INCLUDE, XSD_REDEFINE, XSD_OVERRIDE):
                schema_location = child.get('schemaLocation', '').strip()
                if schema_location:
                    locations.add(schema_location)

        return locations

    def replace_location(self, location: str, repl_location: str) -> None:
        if location == repl_location:
            return

        logger.debug("Replace location %r with %r", location, repl_location)
        repl = f'schemaLocation="{repl_location}"'
        pattern = REPLACE_PATTERN.format(re.escape(location))
        self.text = re.sub(pattern, repl, self.text)
        self.modified = True

    def get_location_path(self, location: str,
                          ref: Union[SchemaType, XMLResource],
                          modify: bool = True) -> LocationPath:
        """
        Return a relative location path for the referred XSD schema, replacing
        the original location in the schema source, if necessary.
        """
        parts: Any

        if is_remote_url(location):
            parts = urlsplit(unquote(location))
            path = LocationPath(parts.scheme). \
                joinpath(parts.netloc). \
                joinpath(parts.path.lstrip('/'))
        else:
            if location.startswith('file:/'):
                path = LocationPath(unquote(urlsplit(location).path))
            else:
                path = LocationPath(unquote(location))

            if not path.is_absolute():
                path = self.path.parent.joinpath(path).normalize()
                if not str(path).startswith('..'):
                    # A relative path that doesn't exceed the loading schema dir
                    return path

                # Use the absolute resource path
                path = LocationPath(ref.filepath)  # type: ignore[arg-type]

            if path.drive:
                drive = path.drive.split(':')[0]
                path = LocationPath(drive).joinpath('/'.join(path.parts[1:]))

            path = LocationPath('file').joinpath(path.as_posix().lstrip('/'))

        if path.is_absolute():
            raise XMLSchemaValueError(f'Replacing path {path} is not relative!')

        # Obtain the replacement location
        parts = path.parent.parts
        dir_parts = self.path.parent.parts

        k = 0
        for item1, item2 in zip(parts, dir_parts):
            if item1 != item2:
                break
            k += 1

        if not k:
            prefix = '/'.join(['..'] * len(dir_parts))
            repl_path = LocationPath(prefix).joinpath(path)
        else:
            repl_path = LocationPath('/'.join(parts[k:])).joinpath(path.name)
            if k < len(dir_parts):
                prefix = '/'.join(['..'] * (len(dir_parts) - k))
                repl_path = LocationPath(prefix).joinpath(repl_path)

        repl_location = repl_path.as_posix()
        if location != repl_location:
            if self.substitutions is None:
                self.substitutions = []
            self.substitutions.append((location, repl_location))

            if modify:
                self.replace_location(location, repl_location)

        return path


def save_sources(target: Union[str, Path],
                 sources: Iterable[XsdSource],
                 save_locations: bool = False) -> dict[str, str]:
    """Save XSD sources to a target directory."""
    target_path = Path(target) if isinstance(target, str) else target
    if target_path.is_dir():
        if list(target_path.iterdir()):
            msg = _("target directory {} is not empty")
            raise XMLSchemaValueError(msg.format(target))
    elif target_path.exists():
        msg = _("target {} is not a directory")
        raise XMLSchemaValueError(msg.format(target_path.parent))
    elif not target_path.parent.exists():
        msg = _("target parent directory {} does not exist")
        raise XMLSchemaValueError(msg.format(target_path.parent))
    elif not target_path.parent.is_dir():
        msg = _("target parent {} is not a directory")
        raise XMLSchemaValueError(msg.format(target_path.parent))

    location_map = {}

    for src in sources:
        assert src.processed

        filepath = target_path.joinpath(src.path)

        # Safety check: raise error if filepath is not inside the target path
        try:
            filepath.resolve(strict=False).relative_to(target_path.resolve(strict=False))
        except ValueError:
            msg = _("target directory {} violation for exported path {}, {}")
            raise XMLSchemaValueError(msg.format(target, str(src.path), str(filepath)))

        if not filepath.parent.exists():
            filepath.parent.mkdir(parents=True)

        encoding = 'utf-8'  # default encoding for XML 1.0

        if src.text.startswith('<?'):
            # Get the encoding from XML declaration
            xml_declaration = src.text.split('\n', maxsplit=1)[0]
            re_match = re.search('(?<=encoding=["\'])[^"\']+', xml_declaration)
            if re_match is not None:
                encoding = re_match.group(0).lower()

        if src.modified:
            logger.info("Write modified XSD source to %s", filepath)
        else:
            logger.info("Write unchanged XSD source to %s", filepath)

        if src.substitutions:
            for location, repl_location in src.substitutions:
                if location not in location_map:
                    location_map[location] = repl_location
                elif repl_location != location_map[location]:
                    logger.warning("Substitution collision for location %r: %r != %r",
                                   location, repl_location, location_map[location])

        with filepath.open(mode='w', encoding=encoding) as fp:
            fp.write(src.text)

    if save_locations:
        with target_path.joinpath('__init__.py').open('w') as fp:
            logger.info("Write LOCATION_MAP to %s", fp.name)
            fp.write(f'LOCATION_MAP = {pprint.pformat(location_map)}')

    return location_map


@logged
def export_schema(schema: SchemaType,
                  target: Union[str, Path],
                  save_remote: bool = False,
                  remove_residuals: bool = True,
                  exclude_locations: Optional[list[str]] = None,
                  loglevel: Optional[Union[str, int]] = None) -> dict[str, str]:
    """
    Export XSD sources used by a schema instance to a target directory.
    Don't use this function directly, use XMLSchema.export() method instead.
    """
    def residuals_filter(x: str) -> bool:
        return is_remote_url(x) and x not in schema.includes and \
            (exclude_locations is None or x not in exclude_locations)

    if loglevel is not None:
        logger.info("Export schema using loglevel %r", loglevel)

    name = schema.name or 'schema.xsd'
    exports = {schema: XsdSource(LocationPath(name), schema.source)}
    path: Any

    if exclude_locations is None:
        exclude_locations = []

    logger.debug("Start export of schema %r", name)

    while True:
        current_length = len(exports)

        for schema in list(exports):
            schema_source = exports[schema]
            if schema_source.processed:
                continue  # Skip already processed schemas

            schema_source.processed = True
            logger.debug("Process schema instance %r", schema)

            schema_locations = schema_source.schema_locations

            imports_items = [(x.url, x) for x in schema.imports.values()
                             if x is not None and x.meta_schema is not None]

            for location, ref_schema in chain(schema.includes.items(), imports_items):
                if not location:
                    continue
                elif location in exclude_locations or not save_remote and is_remote_url(location):
                    logger.debug("Location %r is excluded by argument", location)
                    continue

                # Find matching schema location
                location_match = match_location(location, schema_locations)
                if location_match is None:
                    logger.debug("Unmatched location %r, skip ...", location)
                    continue

                location = location_match
                logger.debug("Matched location %r", location)
                schema_locations.remove(location)

                path = schema_source.get_location_path(location, ref_schema)
                if ref_schema not in exports:
                    exports[ref_schema] = XsdSource(path, ref_schema.source)

            if remove_residuals:
                # Deactivate residual redundant imports from remote URLs
                for location in filter(residuals_filter, schema_locations):
                    logger.debug("Clear residual remote location %r", location)
                    schema_source.replace_location(location, '')

        if current_length == len(exports):
            break

    return save_sources(target, exports.values())


@logged
def download_schemas(url: str,
                     target: Union[str, Path],
                     save_remote: bool = True,
                     save_locations: bool = True,
                     modify: bool = False,
                     defuse: str = 'remote',
                     timeout: int = 300,
                     exclude_locations: Optional[list[str]] = None,
                     loglevel: Optional[Union[str, int]] = None) -> dict[str, str]:
    """
    Download one or more schemas from a URL and save them in a target directory. All the
    referred locations in schema sources are downloaded and stored in the target directory.

    :param url: The URL of the schema to download, usually a remote one.
    :param target: the target directory to save the schema.
    :param save_remote: if to save remote schemas, defaults to `True`.
    :param save_locations: for default save a LOCATION_MAP dictionary to a `__init__.py`, \
    that can be imported in your code to provide a *uri_mapper* argument for build the \
    schema instance. Provide `False` to skip the package file creation in the target \
    directory.
    :param modify: provide `True` to modify original schemas, defaults to `False`.
    :param defuse: when to defuse XML data before loading, defaults to `'remote'`.
    :param timeout: the timeout in seconds for the connection attempt in case of remote data.
    :param exclude_locations: provide a list of locations to skip.
    :param loglevel: for setting a different logging level for schema downloads call.
    :return: a dictionary containing the map of modified locations.
    """
    if loglevel is not None:
        logger.info("Download schemas using loglevel %r", loglevel)

    resource = XMLResource(url, defuse=defuse, timeout=timeout)
    logger.info("Downloaded XML resource from %s", url)
    if resource.root.tag != XSD_SCHEMA:
        raise XMLSchemaValueError(f'Resource referred by {url} is not a XSD schema')

    name = resource.name
    downloads = {
        resource: XsdSource(LocationPath(name), resource)  # type: ignore[arg-type]
    }
    path: Any

    if exclude_locations is None:
        exclude_locations = []

    logger.debug("Start download of schema resource %r", name)

    while True:
        current_length = len(downloads)

        for resource in list(downloads):
            schema_source = downloads[resource]
            if schema_source.processed:
                continue  # Skip already processed schemas

            schema_source.processed = True
            logger.debug("Process schema resource %r", resource)
            schema_locations = schema_source.schema_locations

            for location in schema_locations:
                if location in exclude_locations or not save_remote and is_remote_url(location):
                    logger.debug("Location %r is excluded by argument", location)
                    continue

                url = normalize_url(location, resource.base_url)
                if any(x.url == url for x in downloads):
                    continue

                try:
                    ref_resource = XMLResource(url, defuse=defuse, timeout=timeout)
                except (OSError, XMLResourceOSError) as err:
                    logger.error('Error accessing resource at URL %s: %s', url, err)
                    continue
                except ElementTree.ParseError as err:
                    logger.error('Error parsing XML resource at URL %s: %s', url, err)
                    continue
                else:
                    logger.info("Downloaded XML resource from %s", url)

                if ref_resource.root.tag != XSD_SCHEMA:
                    logger.error('XML resource at URL %s is not an XSD schema', url)
                    continue

                path = schema_source.get_location_path(location, ref_resource, modify)
                downloads[ref_resource] = XsdSource(path, ref_resource)

        if current_length == len(downloads):
            break

    return save_sources(target, downloads.values(), save_locations)
