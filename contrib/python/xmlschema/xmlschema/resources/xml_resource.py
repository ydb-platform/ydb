#
# Copyright (c), 2024-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import importlib.resources
from importlib.resources.abc import Traversable
import io
import os.path
import threading
from collections import deque
from collections.abc import Iterator, MutableMapping
from io import StringIO, BytesIO
from pathlib import Path
from types import TracebackType
from typing import cast, Any, Optional, Union
from urllib.request import urlopen, OpenerDirector
from urllib.parse import urlsplit, unquote
from urllib.error import URLError
from xml.etree import ElementTree

from xmlschema.aliases import SettingsType, ElementType, EtreeType, NsmapType, \
    NormalizedLocationsType, LocationsType, XMLSourceType, IOType, \
    LazyType, IterParseType, UriMapperType, BaseUrlType, BlockType
from xmlschema.exceptions import XMLSchemaTypeError, XMLSchemaValueError, \
    XMLResourceError, XMLResourceOSError, XMLResourceBlocked
from xmlschema.utils.paths import LocationPath
from xmlschema.utils.etree import is_etree_element, etree_tostring, iter_schema_location_hints
from xmlschema.utils.misc import iter_class_slots
from xmlschema.utils.streams import is_file_object
from xmlschema.utils.qnames import update_namespaces, get_namespace_map
from xmlschema.utils.urls import is_url, is_remote_url, is_local_url, normalize_url, \
    normalize_locations
from xmlschema.xpath import ElementSelector
from xmlschema.arguments import Argument, SourceArgument, BaseUrlOption, \
    AllowOption, BlockOption, DefuseOption, PositiveIntOption, UriMapperOption, \
    OpenerOption, SelectorOption

from .sax import defuse_xml
from .xml_loader import XMLResourceLoader


class XMLResourceManager:
    """A context manager for XML resources."""
    def __init__(self, resource: 'XMLResource') -> None:
        self.resource = resource

    def __enter__(self) -> 'XMLResourceManager':
        self.fp = self.resource.open()
        return self

    def __exit__(self, exc_type: Optional[type[BaseException]],
                 exc_value: Optional[BaseException],
                 exc_tb: Optional[TracebackType]) -> None:
        if self.resource.fp is None or not self.resource.fp.seekable():
            self.fp.close()


class XMLResource(XMLResourceLoader):
    """
    XML resource manager based on ElementTree and urllib.

    :param source: a string containing the XML document or file path or a URL or a \
    file like object or an ElementTree or an Element.
    :param base_url: is an optional base URL, used for the normalization of relative paths \
    when the URL of the resource can't be obtained from the source argument. For security \
    the access to a local file resource is always denied if the *base_url* is a remote URL.
    :param allow: defines the security mode for accessing resource locations. Can be \
    'all', 'remote', 'local', 'sandbox' or 'none'. Default is 'all', which means all types \
    of URLs are allowed. With 'remote' only remote resource URLs are allowed. With 'local' \
    only file paths and URLs are allowed. With 'sandbox' only file paths and URLs that \
    are under the directory path identified by the *base_url* argument are allowed. \
    If you provide 'none', no resources will be allowed from any location.
    :param defuse: defines when to defuse XML data using a `SafeXMLParser`. Can be \
    'always', 'remote', 'nonlocal' or 'never'. For default defuses only remote XML data. \
    With 'always' all the XML data that is not already parsed is defused. With 'nonlocal' \
    it defuses unparsed data except local files. With 'never' no XML data source is defused.
    :param timeout: the timeout in seconds for the connection attempt in case of remote data.
    :param lazy: if a value `False` or 0 is provided the XML data is fully loaded into and \
    processed in memory. When a resource is lazy only the root element of the source is \
    loaded. A positive integer also defines the depth at which the lazy resource can be \
    better iterated (`True` means 1).
    :param thin_lazy: for default, in order to reduce the memory usage, during the \
    iteration of a lazy resource at *lazy_depth* level, deletes also the preceding \
    elements after the use.
    :param block: defines which types of sources are blocked for security reasons. \
    For default none of possible types are blocked. Provide a space separated string of \
    words, choosing between 'text', 'file', 'io', 'url' and 'tree' or a tuple of them to \
    select which types are blocked.
    :param uri_mapper: an optional URI mapper for using relocated or URN-addressed \
    resources. Can be a dictionary or a function that takes the URI string and returns \
    a URL, or the argument if there is no mapping for it.
    :param opener: an optional :class:`OpenerDirector` to use for open the resource. \
    For default use the opener installed globally for *urlopen*.
    :param iterparse: an optional callable that returns an iterator parser instance used \
    for building the XML tree. For default that callable is *ElementTree.iterparse*, \
    provide *lxml.etree.iterparse* to build lxml trees or another callable if a \
    different parsing of your data.
    """
    # Descriptor-based attributes for arguments
    source = SourceArgument()

    base_url = BaseUrlOption(default=None)
    allow = AllowOption(default='all')
    defuse = DefuseOption(default='remote')
    timeout = PositiveIntOption(default=300)
    block = BlockOption(default=None)
    uri_mapper = UriMapperOption(default=None)
    opener = OpenerOption(default=None)
    selector = SelectorOption(default=ElementSelector)

    # Private attributes for arguments
    _source: XMLSourceType
    _base_url: Optional[str]
    _allow: str
    _defuse: str
    _timeout: int
    _block: Optional[tuple[str, ...]]
    _uri_mapper: Optional[UriMapperType]
    _opener: Optional[OpenerDirector]
    _selector: type[ElementSelector]

    text: Optional[str] = None
    """The XML text source, `None` if it's not loaded or available."""

    url: Optional[str] = None
    """An URL if the source is an URL or a file-like object with a remote url."""

    fp: Optional[IOType] = None
    """An file-like object if the source is a file-like object."""

    _url_scheme: Optional[str] = None
    _context_fp: Optional[IOType] = None
    _context_lock: threading.Lock = threading.Lock()

    @classmethod
    def from_settings(cls,
                      settings: SettingsType,
                      source: XMLSourceType,
                      **kwargs: Any) -> 'XMLResource':
        """
        Returns a new XMLResource instance from settings. Optional keyword arguments must
        be options for resource initialization and can be passed to override settings.

        :param settings: resource settings.
        :param source: the XML source.
        :param kwargs: additional arguments for resource initialization.
        """
        return settings.get_resource(cls, source, **kwargs)

    def __init__(self, source: XMLSourceType,
                 base_url: Optional[BaseUrlType] = None,
                 allow: str = 'all',
                 defuse: str = 'remote',
                 timeout: int = 300,
                 lazy: LazyType = False,
                 thin_lazy: bool = True,
                 block: Optional[BlockType] = None,
                 uri_mapper: Optional[UriMapperType] = None,
                 opener: Optional[OpenerDirector] = None,
                 iterparse: Optional[IterParseType] = None,
                 selector: Optional[type[ElementSelector]] = None) -> None:

        if allow == 'sandbox' and base_url is None:
            if not is_local_url(source):
                raise XMLSchemaValueError("block access to files out of sandbox requires"
                                          " 'base_url' to be set or a local source URL")
            # Allow sandbox mode without a base_url using the source URL as base
            assert isinstance(source, str)
            base_url = os.path.dirname(normalize_url(source))

        # set and validate arguments
        if isinstance(source, Traversable):
            self.traversal_source = source
            source = source.read_text()

        self.base_url = base_url
        self.allow = allow
        self.defuse = defuse
        self.timeout = timeout
        self.block = block
        self.uri_mapper = uri_mapper
        self.opener = opener
        self.selector = selector
        self.source = source

        if is_url(source):
            assert isinstance(source, (str, bytes, Path))
            self.url = self.get_url(source)
            self._url_scheme = urlsplit(self.url).scheme
            self.access_control(self.url)

        elif isinstance(source, str):
            self.text = source
        elif isinstance(source, StringIO):
            self.text = source.getvalue()
        elif isinstance(source, (bytes, BytesIO)):
            pass
        elif is_file_object(source):
            # source is a file-like object (remote resource or local file)
            self.fp = cast(IOType, source)
            self.access_control(getattr(source, 'url', None))
        elif self._block is not None and 'tree' in self._block:
            raise XMLResourceBlocked(f"block initialization from {type(source)!r}")
        else:
            super().__init__(cast(EtreeType, source), lazy, thin_lazy, iterparse)
            return

        if self._block is not None:
            # Block control
            if 'file' in self._block and self.fp is not None:
                raise XMLResourceBlocked("block initialization from file")
            elif 'url' in self._block and self.url is not None:
                raise XMLResourceBlocked("block initialization from URL")
            elif 'text' in self._block and isinstance(source, (str, bytes)):
                raise XMLResourceBlocked(f"block initialization from {type(source)!r}")
            elif 'io' in self._block and isinstance(source, (StringIO, BytesIO)):
                raise XMLResourceBlocked(f"block initialization from {type(source)!r}")

        with XMLResourceManager(self) as cm:
            super().__init__(cm.fp, lazy, thin_lazy, iterparse)

    def __repr__(self) -> str:
        if self.url:
            return '%s(url=%r)' % (self.__class__.__name__, self.url)
        return super().__repr__()

    @property
    def name(self) -> Optional[str]:
        """
        The source name, is `None` if the instance is created from an Element or a string.
        """
        return None if self.url is None else os.path.basename(unquote(self.url))

    @property
    def filepath(self) -> Optional[str]:
        """
        The resource filepath if the instance is created from a local file, `None` otherwise.
        """
        if self.url:
            url_parts = urlsplit(self.url)
            if url_parts.scheme in ('', 'file'):
                return str(LocationPath.from_uri(self.url))
        return None

    @property
    def lazy_depth(self) -> int:
        """
        The depth at which the XML tree of the resource is fully loaded during iterations
        methods. Is a positive integer for lazy resources and 0 for fully loaded XML trees.
        """
        return int(self._lazy)

    def is_lazy(self) -> bool:
        """Returns `True` if the XML resource is lazy."""
        return bool(self._lazy)

    def is_thin(self) -> bool:
        """Returns `True` if the XML resource is lazy and thin."""
        return bool(self._lazy) and self._thin_lazy

    def is_remote(self) -> bool:
        """Returns `True` if the resource is related with remote XML data."""
        return is_remote_url(self.url)

    def is_local(self) -> bool:
        """Returns `True` if the resource is related with local XML data."""
        return is_local_url(self.url)

    def is_data(self) -> bool:
        """Returns `True` if the instance source argument is a data object."""
        return not isinstance(self._source, (str, bytes, Path)) \
            and not hasattr(self._source, 'read')

    def is_loaded(self) -> bool:
        """Returns `True` if the XML text of the data source is loaded."""
        return self.text is not None

    def is_defused(self) -> bool:
        """Returns `True` if the XML data is defused before parsing."""
        return self._defuse == 'remote' and is_remote_url(self.base_url) \
            or self._defuse == 'nonlocal' and not is_local_url(self.base_url) \
            or self._defuse == 'always'

    def get_url(self, location: Union[str, bytes, Path]) -> str:
        """
        Get the resource URL from a location.

        :param location: The location to get the resource URL from. \
        Can be a URI or file Path object.
        """
        if isinstance(location, str):
            uri = location.strip()
        elif isinstance(location, bytes):
            uri = location.decode().strip()
        else:
            uri = str(location)

        if isinstance(self._uri_mapper, MutableMapping):
            if uri in self._uri_mapper:
                uri = self._uri_mapper[uri]
        elif callable(self._uri_mapper):
            uri = self._uri_mapper(uri)

        return normalize_url(uri, self._base_url)

    def match_location(self, location: str) -> bool:
        """Matches the location with the URL the XML resource. URL schemes are compared """
        if self.url is None:
            return False

        url = self.get_url(location)
        if self._url_scheme in ('http', 'https', 'ftp', 'ftps') and \
                url.startswith(('http://', 'https://', 'ftp://', 'sftp://')) and \
                url[:5] != self.url[:5]:
            url = url.replace(self.url[:5], self.url[:5], 1)

        return self.url == url

    def access_control(self, url: Optional[str]) -> None:
        if self._allow == 'all' or url is None:
            return
        elif self._allow == 'none':
            raise XMLResourceBlocked(f"block access to resource {url}")
        elif self._allow == 'remote':
            if is_local_url(url):
                raise XMLResourceBlocked(f"block access to local resource {url}")
        elif is_remote_url(url):
            raise XMLResourceBlocked(f"block access to remote resource {url}")
        elif self._allow == 'sandbox' and self._base_url is not None:
            if not url.startswith(normalize_url(self._base_url)):
                raise XMLResourceBlocked(f"block access to out of sandbox file {url}")

    def parse(self, source: XMLSourceType, lazy: LazyType = False) -> None:
        """Parse another XML resource and load it into the instance."""
        kwargs = self.get_arguments()
        kwargs['source'] = source
        kwargs['lazy'] = lazy
        other = self.__class__(**kwargs)
        for name in iter_class_slots(self):
            setattr(self, name, getattr(other, name))
        del other

    def get_arguments(self) -> dict[str, Any]:
        """Returns keyword arguments for rebuilding the XML resource."""
        return {k: getattr(self, k) for k, v in self.__class__.__dict__.items()
                if isinstance(v, Argument)}

    def get_text(self) -> str:
        """
        Gets the source text of the XML document. If the source text is not
        available creates an encoded string representation of the XML tree.
        Il the resource is lazy raises a resource error.
        """
        if self.text is not None:
            return self.text
        elif self.url is not None:
            self.load()
            if self.text is not None:
                return self.text

        return self.tostring(xml_declaration=True)

    def tostring(self, namespaces: Optional[MutableMapping[str, str]] = None,
                 indent: str = '', max_lines: Optional[int] = None,
                 spaces_for_tab: int = 4, xml_declaration: bool = False,
                 encoding: str = 'unicode', method: str = 'xml') -> str:
        """
        Serialize an XML resource to a string.

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
        if self._lazy:
            raise XMLResourceError("can't serialize a lazy XML resource")

        if not hasattr(self.root, 'nsmap'):
            namespaces = self.get_namespaces(namespaces, root_only=False)

        _string = etree_tostring(
            elem=self.root,
            namespaces=namespaces,
            indent=indent,
            max_lines=max_lines,
            spaces_for_tab=spaces_for_tab,
            xml_declaration=xml_declaration,
            encoding=encoding,
            method=method
        )
        if isinstance(_string, bytes):  # pragma: no cover
            return _string.decode('utf-8')
        return _string

    def subresource(self, elem: ElementType) -> 'XMLResource':
        """Create an XMLResource instance from a subelement of a non-lazy XML tree."""
        if self._lazy:
            raise XMLResourceError("can't create a subresource from a lazy XML resource")
        elif not is_etree_element(elem):
            raise XMLSchemaTypeError("argument must be an Element instance")

        for e in self.root.iter():  # pragma: no cover
            if e is elem:
                break
        else:
            msg = "{!r} is not an element or the XML resource tree"
            raise XMLSchemaValueError(msg.format(elem))

        resource = XMLResource(elem, self.base_url, self._allow, self._defuse, self._timeout)
        if not hasattr(elem, 'nsmap'):
            for e in elem.iter():
                resource._nsmaps[e] = self._nsmaps[e]

                if e is elem:
                    ns_declarations = [(k, v) for k, v in self._nsmaps[e].items()]
                    if ns_declarations:
                        resource._xmlns[e] = ns_declarations
                elif e in self._xmlns:
                    resource._xmlns[e] = self._xmlns[e]

        return resource

    def open(self, use_loaded: bool = False) -> IOType:
        """
        Returns an opened resource reader object for the instance URL. If the
        source attribute is a seekable file-like object rewind the source and
        return it. If required by configuration the XML resource is defused
        before returning if to the caller.
        """
        def open_url(url: str) -> IOType:
            try:
                if isinstance(url, Traversable):
                    return url.open()
                elif self._opener is not None:
                    return cast(IOType, self._opener.open(url, timeout=self._timeout))
                return cast(IOType, urlopen(url, timeout=self._timeout))
            except URLError as err:
                raise XMLResourceOSError(f"can't access to resource {url!r}: {err.reason}")

        if use_loaded and self.text is not None:
            fp: IOType = StringIO(self.text)
        elif self.fp is not None:
            if self.fp.closed:
                msg = f"can't open {self!r}: its file-like object has been closed"
                raise XMLResourceOSError(msg)
            elif self.fp.seekable() and self.fp.seek(0) != 0:
                msg = f"can't open {self!r}: its file-like object can't be rewound"
                raise XMLResourceOSError(msg)
            else:
                fp = self.fp

        elif self.url is not None:
            fp = open_url(self.url)
        elif isinstance(self._source, str):
            fp = StringIO(self._source)
        elif isinstance(self._source, bytes):
            fp = BytesIO(self._source)
        elif isinstance(self._source, StringIO):
            fp = StringIO(self._source.getvalue())
        elif isinstance(self._source, BytesIO):
            fp = BytesIO(self._source.getvalue())
        else:
            msg = f"can't open {self!r}: its source is an ElementTree structure"
            raise XMLResourceError(msg)

        if self.is_defused():
            if fp.seekable() or isinstance(fp, (io.RawIOBase, io.BufferedIOBase)) and \
                    (self._opener is None or self.url is None):
                # For seekable file-like objects or ones that can be wrapped in
                # a buffered reader defuse with rewind option if no custom opener
                # is provided and the instance has a url, otherwise fallback to
                # double opening with no rewind after the defusing.
                try:
                    return defuse_xml(fp)
                except XMLResourceError:
                    if self.fp is None:
                        fp.close()
                    raise
            elif self.url is not None:
                # If the file-like object is created from a URL, create a new
                # file-like object for defusing XML data. On remote data this
                # method is less safe.
                with open_url(self.url) as _fp:
                    defuse_xml(_fp, rewind=False)
            else:
                msg = f"can't defuse {self!r}: its file-like object is not seekable"
                raise XMLResourceOSError(msg)

        return fp

    def seek(self, position: int) -> Optional[int]:
        """
        Change stream position if the XML resource was created with a seekable
        file-like object. In the other cases this method has no effect.
        """
        return self.fp.seek(position) if self.fp is not None and self.fp.seekable() else None

    def close(self) -> None:
        """
        Close the XML resource if it's created with a file-like object.
        In other cases this method has no effect.
        """
        if self.fp is not None:
            self.fp.close()

    def load(self) -> None:
        """
        Loads the XML text from the data source. If the data source is an Element
        the source XML text can't be retrieved.
        """
        if self.url is None and not hasattr(self._source, 'read') and \
                not isinstance(self._source, bytes):
            return  # Created from Element or text source --> already loaded
        elif self._lazy:
            raise XMLResourceError("can't load a lazy XML resource")

        with XMLResourceManager(self) as cm:
            data = cm.fp.read()

        if isinstance(data, bytes):
            try:
                text = data.decode('utf-8')
            except UnicodeDecodeError:
                text = data.decode('iso-8859-1')
        else:
            text = data

        self.text = text

    def iter(self, tag: Optional[str] = None) -> Iterator[ElementType]:
        """
        XML resource tree iterator. If tag is not None or '*', only elements whose
        tag equals tag are returned from the iterator. In a lazy resource the yielded
        elements are full over or at *lazy_depth* level, otherwise are incomplete and
        thin for default.
        """
        if not self._lazy:
            yield from self.root.iter(tag)
            return

        tag = '*' if tag is None else tag.strip()
        lazy_depth = int(self._lazy)
        subtree_elements: deque[ElementType] = deque()
        ancestors = []
        level = 0

        with XMLResourceManager(self) as cm:
            for event, node in self._lazy_iterparse(cm.fp):
                if event == "start":
                    if level < lazy_depth:
                        if level:
                            ancestors.append(node)
                        if tag == '*' or node.tag == tag:
                            yield node  # an incomplete element
                    level += 1
                else:
                    level -= 1
                    if level < lazy_depth:
                        if level:
                            ancestors.pop()
                        continue  # pragma: no cover
                    elif level > lazy_depth:
                        if tag == '*' or node.tag == tag:
                            subtree_elements.appendleft(node)
                        continue  # pragma: no cover

                    if tag == '*' or node.tag == tag:
                        yield node  # a full element

                    yield from subtree_elements
                    subtree_elements.clear()

                    self._clear(node, ancestors)

    def iter_location_hints(self, tag: Optional[str] = None) -> Iterator[tuple[str, str]]:
        """
        Yields all schema location hints of the XML resource. If tag
        is not None or '*', only location hints of elements whose tag
        equals tag are returned from the iterator.
        """
        for elem in self.iter(tag):
            yield from iter_schema_location_hints(elem)

    def iter_depth(self, mode: int = 1, ancestors: Optional[list[ElementType]] = None) \
            -> Iterator[ElementType]:
        """
        Iterates XML subtrees. For fully loaded resources yields the root element.
        On lazy resources the argument *mode* can change the sequence and the
        completeness of yielded elements. There are four possible modes, that
        generate different sequences of elements:\n
          1. Only the elements at *depth_level* level of the tree\n
          2. Only the elements at *depth_level* level of the tree removing\n
             the preceding elements of ancestors (thin lazy tree)
          3. Only a root element pruned at *depth_level*\n
          4. The elements at *depth_level* and then a pruned root\n
          5. An incomplete root at start, the elements at *depth_level* and a pruned root\n

        :param mode: an integer in range [1..5] that defines the iteration mode.
        :param ancestors: provide a list for tracking the ancestors of yielded elements.
        """
        if mode not in (1, 2, 3, 4, 5):
            raise XMLSchemaValueError(f"invalid argument mode={mode!r}")

        if ancestors is not None:
            ancestors.clear()
        elif mode <= 2:
            ancestors = []

        if not self._lazy:
            yield self.root
            return

        level = 0
        lazy_depth = int(self._lazy)

        # boolean flags
        incomplete_root = mode == 5
        pruned_root = mode > 2
        depth_level_elements = mode != 3
        thin_lazy = mode <= 2

        with XMLResourceManager(self) as cm:
            for event, elem in self._lazy_iterparse(cm.fp):
                if event == "start":
                    if not level:
                        if incomplete_root:
                            yield elem
                    if ancestors is not None and level < lazy_depth:
                        ancestors.append(elem)
                    level += 1
                else:
                    level -= 1
                    if not level:
                        if pruned_root:
                            yield elem
                        continue
                    elif level != lazy_depth:
                        if ancestors is not None and level < lazy_depth:
                            ancestors.pop()
                        continue  # pragma: no cover
                    elif depth_level_elements:
                        yield elem

                    if thin_lazy:
                        self._clear(elem, ancestors)
                    else:
                        self._clear(elem)

                    if self._xpath_root is not None:
                        self.xpath_root.children.clear()

    def iterfind(self, path: str,
                 namespaces: Optional[NsmapType] = None,
                 ancestors: Optional[list[ElementType]] = None) -> Iterator[ElementType]:
        """
        Apply XPath selection to XML resource that yields full subtrees.

        :param path: an XPath 2.0 expression that selects element nodes. \
        Selecting other values or nodes raise an error.
        :param namespaces: an optional mapping from namespace prefixes to URIs \
        used for parsing the XPath expression.
        :param ancestors: provide a list for tracking the ancestors of yielded elements.
        """
        selector = self._selector.cached_selector(path, namespaces)

        if not self._lazy:
            if ancestors is None:
                yield from selector.iter_select(self)
            else:
                for elem in selector.iter_select(self):
                    if elem is self.root:
                        ancestors.clear()
                    else:
                        _ancestors: Any = []
                        parent = self.parent_map[elem]
                        while parent is not None:
                            _ancestors.append(parent)
                            parent = self.parent_map[parent]

                        if _ancestors:
                            ancestors.clear()
                            ancestors.extend(reversed(_ancestors))
                    yield elem

            return

        lazy_depth = int(self._lazy)
        path_depth = selector.depth
        if path_depth < 1:
            raise XMLSchemaValueError(f"can't use path {path!r} on a lazy resource")
        elif path_depth < lazy_depth:
            raise XMLSchemaValueError(f"can't use path {path!r} on a lazy resource "
                                      f"with lazy_depth=={lazy_depth}")
        select_all = selector.select_all
        level = 0

        if ancestors is not None:
            ancestors.clear()
        elif self._thin_lazy:
            ancestors = []

        with XMLResourceManager(self) as cm:
            for event, node in self._lazy_iterparse(cm.fp):
                if event == "start":
                    if ancestors is not None and level < path_depth:
                        ancestors.append(node)
                    level += 1
                else:
                    level -= 1
                    if level < path_depth:
                        if ancestors is not None:
                            ancestors.pop()
                        continue
                    elif level == path_depth:
                        if select_all or node in selector.iter_select(self):
                            yield node
                    if level == lazy_depth:
                        self._clear(node, ancestors)

    def find(self, path: str,
             namespaces: Optional[NsmapType] = None,
             ancestors: Optional[list[ElementType]] = None) -> Optional[ElementType]:
        return next(self.iterfind(path, namespaces, ancestors), None)

    def findall(self, path: str, namespaces: Optional[NsmapType] = None) \
            -> list[ElementType]:
        return list(self.iterfind(path, namespaces))

    def findtext(self, path: str,
                 default: Optional[str] = None,
                 namespaces: Optional[NsmapType] = None) -> Optional[str]:
        for elem in self.iterfind(path, namespaces):
            return '' if elem.text is None else elem.text
        else:
            return default

    def get_namespaces(self, namespaces: Optional[NsmapType] = None,
                       root_only: bool = True,
                       root_default: bool = False) -> dict[str, str]:
        """
        Extracts namespaces with related prefixes from the XML resource.
        If a duplicate prefix is encountered in a xmlns declaration, and
        this is mapped to a different namespace, adds the namespace using
        a different generated prefix. The empty prefix '' is used only if
        it's declared at root level to avoid erroneous mapping of local
        names. In other cases it uses the prefix 'default' as substitute.

        :param namespaces: is an optional mapping from namespace prefix to URI that \
        integrate/override the namespace declarations of the root element.
        :param root_only: if `True` extracts only the namespaces declared in the root \
        element, otherwise scan the whole tree for further namespace declarations. \
        A full namespace map can be useful for cases where the element context is \
        not available.
        :param root_default: if `True` insert default namespace declaration to no \
        namespace if it's not declared in the root element. Used for getting the \
        right default namespace declaration for schemas.
        :return: a dictionary for mapping namespace prefixes to full URI.
        """
        namespaces = get_namespace_map(namespaces)
        try:
            descendants = self.iter()
            root = next(descendants)
            if root in self._xmlns:
                update_namespaces(namespaces, self._xmlns[root], True)
            if root_default and '' not in namespaces:
                namespaces[''] = ''

            if not root_only:
                for elem in descendants:
                    if elem in self._xmlns:
                        update_namespaces(namespaces, self._xmlns[elem], False)

        except (ElementTree.ParseError, UnicodeEncodeError):
            return namespaces  # a lazy resource with malformed XML data
        else:
            return namespaces

    def get_locations(self, locations: Optional[LocationsType] = None,
                      root_only: bool = True) -> NormalizedLocationsType:
        """
        Extracts a list of schema location hints from the XML resource.
        The locations are normalized using the base URL of the instance.

        :param locations: a sequence of schema location hints inserted \
        before the ones extracted from the XML resource. Locations passed \
        within a tuple container are not normalized.
        :param root_only: if `True` extracts only the location hints of the \
        root element.
        :returns: a list of couples containing normalized location hints.
        """
        if not locations:
            location_hints = []
        elif isinstance(locations, tuple):
            location_hints = [x for x in locations]
        else:
            location_hints = normalize_locations(locations, self.base_url)

        if root_only:
            location_hints.extend([
                (ns, normalize_url(url, self.base_url))
                for ns, url in iter_schema_location_hints(self.root)
            ])
        else:
            try:
                location_hints.extend([
                    (ns, normalize_url(url, self.base_url))
                    for ns, url in self.iter_location_hints()
                ])
            except (ElementTree.ParseError, UnicodeEncodeError):
                pass  # a lazy resource containing malformed XML data after the first tag

        return location_hints
