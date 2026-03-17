# Hand-written stub for lxml.etree as used by mypy.report.
# This is *far* from complete, and the stubgen-generated ones crash mypy.
# Any use of `Any` below means I couldn't figure out the type.

from os import PathLike
from typing import (
    IO,
    Any,
    Callable,
    Collection,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Sized,
    SupportsBytes,
    Tuple,
    Type,
    TypeVar,
    Union,
    overload,
)

from typing_extensions import Literal, Protocol, SupportsIndex, TypeAlias, TypeGuard

# dummy for missing stubs
def __getattr__(name: str) -> Any: ...

# We do *not* want `typing.AnyStr` because it is a `TypeVar`, which is an
# unnecessary constraint. It seems reasonable to constrain each
# List/Dict argument to use one type consistently, though, and it is
# necessary in order to keep these brief.
_AnyStr = Union[str, bytes]
_AnySmartStr = Union[
    "_ElementUnicodeResult", "_PyElementUnicodeResult", "_ElementStringResult"
]
_TagName = Union[str, bytes, QName]
# _TagSelector also allows Element, Comment, ProcessingInstruction
_TagSelector = Union[_TagName, Collection[_TagSelector], Any]
# XPath object - http://lxml.de/xpathxslt.html#xpath-return-values
_XPathObject = Union[
    bool,
    float,
    _AnySmartStr,
    _AnyStr,
    List[
        Union[
            "_Element",
            _AnySmartStr,
            _AnyStr,
            Tuple[Optional[_AnyStr], Optional[_AnyStr]],
        ]
    ],
]
_AnyParser = Union["XMLParser", "HTMLParser"]
_ListAnyStr = Union[List[str], List[bytes]]
_DictAnyStr = Union[Dict[str, str], Dict[bytes, bytes]]
_Dict_Tuple2AnyStr_Any = Union[Dict[Tuple[str, str], Any], Tuple[bytes, bytes], Any]
_xpath = Union["XPath", _AnyStr]

# See https://github.com/python/typing/pull/273
# Due to Mapping having invariant key types, Mapping[Union[A, B], ...]
# would fail to validate against either Mapping[A, ...] or Mapping[B, ...]
# Try to settle for simpler solution, encouraging use of empty string ('')
# as default namespace prefix. If too many people complain, it can be
# back-paddled as Mapping[Any, ...]
_NSMapArg = Mapping[str, str]
_NonDefaultNSMapArg = Mapping[str, str]  # empty prefix disallowed

_T = TypeVar("_T")
_KnownEncodings = Literal[
    "ASCII",
    "ascii",
    "UTF-8",
    "utf-8",
    "UTF8",
    "utf8",
    "US-ASCII",
    "us-ascii",
]
_ElementOrTree = Union[_Element, _ElementTree]
_FileSource = Union[_AnyStr, IO[Any], PathLike[Any]]

class ElementChildIterator(Iterator["_Element"]):
    def __iter__(self) -> "ElementChildIterator": ...
    def __next__(self) -> "_Element": ...

class _ElementUnicodeResult(str):
    is_attribute: bool
    is_tail: bool
    is_text: bool
    attrname: Optional[_AnyStr]
    def getparent(self) -> Optional["_Element"]: ...

class _PyElementUnicodeResult(str):
    is_attribute: bool
    is_tail: bool
    is_text: bool
    attrname: Optional[_AnyStr]
    def getparent(self) -> Optional["_Element"]: ...

class _ElementStringResult(bytes):
    is_attribute: bool
    is_tail: bool
    is_text: bool
    attrname: Optional[_AnyStr]
    def getparent(self) -> Optional["_Element"]: ...

class DocInfo:
    root_name = ...  # type: str
    public_id = ...  # type: Optional[str]
    system_id = ...  # type: Optional[str]
    xml_version = ...  # type: Optional[str]
    encoding = ...  # type: Optional[str]
    standalone = ...  # type: Optional[bool]
    URL = ...  # type: str
    internalDTD = ...  # type: "DTD"
    externalDTD = ...  # type: "DTD"
    def __init__(self, tree: Union["_ElementTree", "_Element"]) -> None: ...
    def clear(self) -> None: ...

class _Element(Iterable["_Element"], Sized):
    def __delitem__(self, key: Union[int, slice]) -> None: ...
    def __getitem__(self, item: int) -> _Element: ...
    @overload
    def __setitem__(self, __key: SupportsIndex, value: _Element) -> None: ...
    @overload
    def __setitem__(self, __key: slice, value: Iterable[_Element]) -> None: ...
    def __iter__(self) -> ElementChildIterator: ...
    def __len__(self) -> int: ...
    def addprevious(self, element: "_Element") -> None: ...
    def addnext(self, element: "_Element") -> None: ...
    def append(self, element: "_Element") -> None: ...
    def cssselect(self, expression: str) -> List[_Element]: ...
    def extend(self, elements: Iterable["_Element"]) -> None: ...
    def find(
        self, path: str, namespaces: Optional[_NSMapArg] = ...
    ) -> Optional["_Element"]: ...
    @overload
    def findtext(
        self,
        path: str,
        namespaces: Optional[_NSMapArg] = ...,
    ) -> Optional[str]: ...
    @overload
    def findtext(
        self,
        path: str,
        default: _T = ...,
        namespaces: Optional[_NSMapArg] = ...,
    ) -> Union[str, _T]: ...
    def findall(
        self, path: str, namespaces: Optional[_NSMapArg] = ...
    ) -> List["_Element"]: ...
    def clear(self) -> None: ...
    @overload
    def get(self, key: _TagName) -> Optional[str]: ...
    @overload
    def get(self, key: _TagName, default: _T) -> Union[str, _T]: ...
    def getnext(self) -> Optional[_Element]: ...
    def getparent(self) -> Optional[_Element]: ...
    def getprevious(self) -> Optional[_Element]: ...
    def getroottree(self) -> _ElementTree: ...
    def index(
        self, child: _Element, start: Optional[int] = ..., stop: Optional[int] = ...
    ) -> int: ...
    def insert(self, index: int, element: _Element) -> None: ...
    def items(self) -> Sequence[Tuple[_AnyStr, _AnyStr]]: ...
    def iter(
        self, tag: Optional[_TagSelector] = ..., *tags: _TagSelector
    ) -> Iterator[_Element]: ...
    iterancestors = iter
    def iterchildren(
        self,
        tag: Optional[_TagSelector] = ...,
        *tags: _TagSelector,
        reversed: bool = False,
    ) -> Iterator[_Element]: ...
    iterdescendants = iter
    def iterfind(
        self, path: str, namespaces: Optional[_NSMapArg] = ...
    ) -> Iterator["_Element"]: ...
    def itersiblings(
        self,
        tag: Optional[_TagSelector] = ...,
        *tags: _TagSelector,
        preceding: bool = False,
    ) -> Iterator[_Element]: ...
    def itertext(
        self,
        tag: Optional[_TagSelector] = ...,
        *tags: _TagSelector,
        with_tail: bool = False,
    ) -> Iterator[_AnyStr]: ...
    def keys(self) -> Sequence[_AnyStr]: ...
    def makeelement(
        self,
        _tag: _TagName,
        attrib: Optional[_DictAnyStr] = ...,
        nsmap: Optional[_NSMapArg] = ...,
        **_extra: Any,
    ) -> _Element: ...
    def remove(self, element: _Element) -> None: ...
    def replace(self, old_element: _Element, new_element: _Element) -> None: ...
    def set(self, key: _TagName, value: _AnyStr) -> None: ...
    def values(self) -> Sequence[_AnyStr]: ...
    def xpath(
        self,
        _path: _AnyStr,
        namespaces: Optional[_NonDefaultNSMapArg] = ...,
        extensions: Any = ...,
        smart_strings: bool = ...,
        **_variables: _XPathObject,
    ) -> _XPathObject: ...
    tag = ...  # type: str
    attrib = ...  # type: _Attrib
    text = ...  # type: Optional[str]
    tail = ...  # type: Optional[str]
    prefix = ...  # type: str
    sourceline = ...  # Optional[int]
    @property
    def nsmap(self) -> Dict[Optional[str], str]: ...
    base = ...  # type: Optional[str]

class ElementBase(_Element): ...

class _ElementTree:
    parser = ...  # type: _AnyParser
    docinfo = ...  # type: DocInfo
    def find(
        self, path: str, namespaces: Optional[_NSMapArg] = ...
    ) -> Optional["_Element"]: ...
    def findtext(
        self,
        path: str,
        default: Optional[str] = ...,
        namespaces: Optional[_NSMapArg] = ...,
    ) -> Optional[str]: ...
    def findall(
        self, path: str, namespaces: Optional[_NSMapArg] = ...
    ) -> List["_Element"]: ...
    def getpath(self, element: _Element) -> str: ...
    def getelementpath(self, element: _Element) -> str: ...
    def getroot(self) -> _Element: ...
    def iter(
        self, tag: Optional[_TagSelector] = ..., *tags: _TagSelector
    ) -> Iterator[_Element]: ...
    def iterfind(
        self, path: str, namespaces: Optional[_NSMapArg] = ...
    ) -> Iterator["_Element"]: ...
    def parse(
        self,
        source: _FileSource,
        parser: Optional[_AnyParser] = ...,
        base_url: Optional[_AnyStr] = ...,
    ) -> _Element: ...
    def write(
        self,
        file: _FileSource,
        encoding: _AnyStr = ...,
        method: _AnyStr = ...,
        pretty_print: bool = ...,
        xml_declaration: Any = ...,
        with_tail: Any = ...,
        standalone: bool = ...,
        compression: int = ...,
        exclusive: bool = ...,
        with_comments: bool = ...,
        inclusive_ns_prefixes: _ListAnyStr = ...,
    ) -> None: ...
    def write_c14n(
        self,
        file: _FileSource,
        with_comments: bool = ...,
        compression: int = ...,
        inclusive_ns_prefixes: Iterable[_AnyStr] = ...,
    ) -> None: ...
    def _setroot(self, root: _Element) -> None: ...
    def xinclude(self) -> None: ...
    def xpath(
        self,
        _path: _AnyStr,
        namespaces: Optional[_NonDefaultNSMapArg] = ...,
        extensions: Any = ...,
        smart_strings: bool = ...,
        **_variables: _XPathObject,
    ) -> _XPathObject: ...
    def xslt(
        self,
        _xslt: XSLT,
        extensions: Optional[_Dict_Tuple2AnyStr_Any] = ...,
        access_control: Optional[XSLTAccessControl] = ...,
        **_variables: Any,
    ) -> _ElementTree: ...

class __ContentOnlyEleement(_Element): ...
class _Comment(__ContentOnlyEleement): ...

class _ProcessingInstruction(__ContentOnlyEleement):
    target: _AnyStr

class _Attrib:
    def __setitem__(self, key: _AnyStr, value: _AnyStr) -> None: ...
    def __delitem__(self, key: _AnyStr) -> None: ...
    def update(
        self,
        sequence_or_dict: Union[
            _Attrib, Mapping[_AnyStr, _AnyStr], Sequence[Tuple[_AnyStr, _AnyStr]]
        ],
    ) -> None: ...
    @overload
    def pop(self, key: _AnyStr) -> _AnyStr: ...
    @overload
    def pop(self, key: _AnyStr, default: _AnyStr) -> _AnyStr: ...
    def clear(self) -> None: ...
    def __repr__(self) -> str: ...
    def __copy__(self) -> _DictAnyStr: ...
    def __deepcopy__(self, memo: Dict[Any, Any]) -> _DictAnyStr: ...
    def __getitem__(self, key: _AnyStr) -> _AnyStr: ...
    def __bool__(self) -> bool: ...
    def __len__(self) -> int: ...
    @overload
    def get(self, key: _TagName) -> Optional[str]: ...
    @overload
    def get(self, key: _TagName, default: _T) -> Union[str, _T]: ...
    def keys(self) -> _ListAnyStr: ...
    def __iter__(self) -> Iterator[_AnyStr]: ...  # actually _AttribIterator
    def iterkeys(self) -> Iterator[_AnyStr]: ...
    def values(self) -> _ListAnyStr: ...
    def itervalues(self) -> Iterator[_AnyStr]: ...
    def items(self) -> List[Tuple[_AnyStr, _AnyStr]]: ...
    def iteritems(self) -> Iterator[Tuple[_AnyStr, _AnyStr]]: ...
    def has_key(self, key: _AnyStr) -> bool: ...
    def __contains__(self, key: _AnyStr) -> bool: ...
    def __richcmp__(self, other: _Attrib, op: int) -> bool: ...

class QName:
    localname = ...  # type: str
    namespace = ...  # type: str
    text = ...  # type: str
    def __init__(
        self,
        text_or_uri_element: Union[None, _AnyStr, _Element],
        tag: Optional[_AnyStr] = ...,
    ) -> None: ...

class _XSLTResultTree(_ElementTree, SupportsBytes):
    def __bytes__(self) -> bytes: ...

class _XSLTQuotedStringParam: ...

# https://lxml.de/parsing.html#the-target-parser-interface
class ParserTarget(Protocol):
    def comment(self, text: _AnyStr) -> None: ...
    def close(self) -> Any: ...
    def data(self, data: _AnyStr) -> None: ...
    def end(self, tag: _AnyStr) -> None: ...
    def start(self, tag: _AnyStr, attrib: Dict[_AnyStr, _AnyStr]) -> None: ...

class ElementClassLookup: ...

class FallbackElementClassLookup(ElementClassLookup):
    fallback: Optional[ElementClassLookup]
    def __init__(self, fallback: Optional[ElementClassLookup] = ...): ...
    def set_fallback(self, lookup: ElementClassLookup) -> None: ...

class CustomElementClassLookup(FallbackElementClassLookup):
    def lookup(
        self, type: str, doc: str, namespace: str, name: str
    ) -> Optional[Type[ElementBase]]: ...

class _BaseParser:
    def __getattr__(self, name: str) -> Any: ...  # Incomplete
    def copy(self) -> _BaseParser: ...
    def makeelement(
        self,
        _tag: _TagName,
        attrib: Optional[Union[_DictAnyStr, _Attrib]] = ...,
        nsmap: Optional[_NSMapArg] = ...,
        **_extra: Any,
    ) -> _Element: ...
    def setElementClassLookup(
        self, lookup: Optional[ElementClassLookup] = ...
    ) -> None: ...
    def set_element_class_lookup(
        self, lookup: Optional[ElementClassLookup] = ...
    ) -> None: ...

class _FeedParser(_BaseParser):
    def __getattr__(self, name: str) -> Any: ...  # Incomplete
    def close(self) -> _Element: ...
    def feed(self, data: _AnyStr) -> None: ...

class XMLParser(_FeedParser):
    def __init__(
        self,
        encoding: Optional[_AnyStr] = ...,
        attribute_defaults: bool = ...,
        dtd_validation: bool = ...,
        load_dtd: bool = ...,
        no_network: bool = ...,
        ns_clean: bool = ...,
        recover: bool = ...,
        schema: Optional[XMLSchema] = ...,
        huge_tree: bool = ...,
        remove_blank_text: bool = ...,
        resolve_entities: bool = ...,
        remove_comments: bool = ...,
        remove_pis: bool = ...,
        strip_cdata: bool = ...,
        collect_ids: bool = ...,
        target: Optional[ParserTarget] = ...,
        compact: bool = ...,
    ) -> None: ...
    resolvers = ...  # type: _ResolverRegistry

class HTMLParser(_FeedParser):
    def __init__(
        self,
        encoding: Optional[_AnyStr] = ...,
        collect_ids: bool = ...,
        compact: bool = ...,
        huge_tree: bool = ...,
        no_network: bool = ...,
        recover: bool = ...,
        remove_blank_text: bool = ...,
        remove_comments: bool = ...,
        remove_pis: bool = ...,
        schema: Optional[XMLSchema] = ...,
        strip_cdata: bool = ...,
        target: Optional[ParserTarget] = ...,
    ) -> None: ...

class _ResolverRegistry:
    def add(self, resolver: Resolver) -> None: ...
    def remove(self, resolver: Resolver) -> None: ...

class Resolver:
    def resolve(self, system_url: str, public_id: str): ...
    def resolve_file(
        self, f: IO[Any], context: Any, *, base_url: Optional[_AnyStr], close: bool
    ): ...
    def resolve_string(
        self, string: _AnyStr, context: Any, *, base_url: Optional[_AnyStr]
    ): ...

class XMLSchema(_Validator):
    def __init__(
        self,
        etree: _ElementOrTree = ...,
        file: _FileSource = ...,
    ) -> None: ...
    def __call__(self, etree: _ElementOrTree) -> bool: ...

class XSLTAccessControl: ...

class XSLT:
    def __init__(
        self,
        xslt_input: _ElementOrTree,
        extensions: _Dict_Tuple2AnyStr_Any = ...,
        regexp: bool = ...,
        access_control: XSLTAccessControl = ...,
    ) -> None: ...
    def __call__(
        self,
        _input: _ElementOrTree,
        profile_run: bool = ...,
        **kwargs: Union[_AnyStr, _XSLTQuotedStringParam],
    ) -> _XSLTResultTree: ...
    @staticmethod
    def strparam(s: _AnyStr) -> _XSLTQuotedStringParam: ...

def Comment(text: Optional[_AnyStr] = ...) -> _Comment: ...
def Element(
    _tag: _TagName,
    attrib: Optional[_DictAnyStr] = ...,
    nsmap: Optional[_NSMapArg] = ...,
    **extra: _AnyStr,
) -> _Element: ...
def SubElement(
    _parent: _Element,
    _tag: _TagName,
    attrib: Optional[_DictAnyStr] = ...,
    nsmap: Optional[_NSMapArg] = ...,
    **extra: _AnyStr,
) -> _Element: ...
def ElementTree(
    element: _Element = ...,
    file: _FileSource = ...,
    parser: Optional[_AnyParser] = ...,
) -> _ElementTree: ...
def ProcessingInstruction(
    target: _AnyStr, text: _AnyStr = ...
) -> _ProcessingInstruction: ...

PI = ProcessingInstruction

def HTML(
    text: _AnyStr,
    parser: Optional[HTMLParser] = ...,
    base_url: Optional[_AnyStr] = ...,
) -> _Element: ...
def XML(
    text: _AnyStr,
    parser: Optional[XMLParser] = ...,
    base_url: Optional[_AnyStr] = ...,
) -> _Element: ...
def cleanup_namespaces(
    tree_or_element: _ElementOrTree,
    top_nsmap: Optional[_NSMapArg] = ...,
    keep_ns_prefixes: Optional[Iterable[_AnyStr]] = ...,
) -> None: ...
def parse(
    source: _FileSource,
    parser: Optional[_AnyParser] = ...,
    base_url: _AnyStr = ...,
) -> Union[_ElementTree, Any]: ...
@overload
def fromstring(
    text: _AnyStr,
    parser: None = ...,
    *,
    base_url: _AnyStr = ...,
) -> _Element: ...
@overload
def fromstring(
    text: _AnyStr,
    parser: _AnyParser = ...,
    *,
    base_url: _AnyStr = ...,
) -> Union[_Element, Any]: ...
@overload
def tostring(
    element_or_tree: _ElementOrTree,
    encoding: Union[Type[str], Literal["unicode"]],
    method: str = ...,
    xml_declaration: bool = ...,
    pretty_print: bool = ...,
    with_tail: bool = ...,
    standalone: bool = ...,
    doctype: str = ...,
    exclusive: bool = ...,
    with_comments: bool = ...,
    inclusive_ns_prefixes: Any = ...,
) -> str: ...
@overload
def tostring(
    element_or_tree: _ElementOrTree,
    # Should be anything but "unicode", cannot be typed
    encoding: Optional[_KnownEncodings] = None,
    method: str = ...,
    xml_declaration: bool = ...,
    pretty_print: bool = ...,
    with_tail: bool = ...,
    standalone: bool = ...,
    doctype: str = ...,
    exclusive: bool = ...,
    with_comments: bool = ...,
    inclusive_ns_prefixes: Any = ...,
) -> bytes: ...
@overload
def tostring(
    element_or_tree: _ElementOrTree,
    encoding: Union[str, type] = ...,
    method: str = ...,
    xml_declaration: bool = ...,
    pretty_print: bool = ...,
    with_tail: bool = ...,
    standalone: bool = ...,
    doctype: str = ...,
    exclusive: bool = ...,
    with_comments: bool = ...,
    inclusive_ns_prefixes: Any = ...,
) -> _AnyStr: ...

class _ErrorLog: ...
class Error(Exception): ...

class LxmlError(Error):
    def __init__(self, message: Any, error_log: _ErrorLog = ...) -> None: ...
    error_log = ...  # type: _ErrorLog

class DocumentInvalid(LxmlError): ...
class LxmlSyntaxError(LxmlError, SyntaxError): ...

class ParseError(LxmlSyntaxError):
    position: Tuple[int, int]

class XMLSyntaxError(ParseError): ...

class _Validator:
    def assert_(self, etree: _ElementOrTree) -> None: ...
    def assertValid(self, etree: _ElementOrTree) -> None: ...
    def validate(self, etree: _ElementOrTree) -> bool: ...
    error_log = ...  # type: _ErrorLog

class DTD(_Validator):
    def __init__(self, file: _FileSource = ..., *, external_id: Any = ...) -> None: ...
    def __call__(self, etree: _ElementOrTree) -> bool: ...

class _XPathEvaluatorBase: ...

class XPath(_XPathEvaluatorBase):
    def __init__(
        self,
        path: _AnyStr,
        *,
        namespaces: Optional[_NonDefaultNSMapArg] = ...,
        extensions: Any = ...,
        regexp: bool = ...,
        smart_strings: bool = ...,
    ) -> None: ...
    def __call__(
        self, _etree_or_element: _ElementOrTree, **_variables: _XPathObject
    ) -> _XPathObject: ...
    path = ...  # type: str

class ETXPath(XPath):
    def __init__(
        self,
        path: _AnyStr,
        *,
        extensions: Any = ...,
        regexp: bool = ...,
        smart_strings: bool = ...,
    ) -> None: ...

class XPathElementEvaluator(_XPathEvaluatorBase):
    def __init__(
        self,
        element: _Element,
        *,
        namespaces: Optional[_NonDefaultNSMapArg] = ...,
        extensions: Any = ...,
        regexp: bool = ...,
        smart_strings: bool = ...,
    ) -> None: ...
    def __call__(self, _path: _AnyStr, **_variables: _XPathObject) -> _XPathObject: ...
    def register_namespace(self, prefix: _AnyStr, uri: _AnyStr) -> None: ...
    def register_namespaces(
        self, namespaces: Optional[_NonDefaultNSMapArg]
    ) -> None: ...

class XPathDocumentEvaluator(XPathElementEvaluator):
    def __init__(
        self,
        etree: _ElementTree,
        *,
        namespaces: Optional[_NonDefaultNSMapArg] = ...,
        extensions: Any = ...,
        regexp: bool = ...,
        smart_strings: bool = ...,
    ) -> None: ...

@overload
def XPathEvaluator(
    etree_or_element: _Element,
    namespaces: Optional[_NonDefaultNSMapArg] = ...,
    extensions: Any = ...,
    regexp: bool = ...,
    smart_strings: bool = ...,
) -> XPathElementEvaluator: ...
@overload
def XPathEvaluator(
    etree_or_element: _ElementTree,
    namespaces: Optional[_NonDefaultNSMapArg] = ...,
    extensions: Any = ...,
    regexp: bool = ...,
    smart_strings: bool = ...,
) -> XPathDocumentEvaluator: ...
@overload
def XPathEvaluator(
    etree_or_element: _ElementOrTree,
    namespaces: Optional[_NonDefaultNSMapArg] = ...,
    extensions: Any = ...,
    regexp: bool = ...,
    smart_strings: bool = ...,
) -> Union[XPathElementEvaluator, XPathDocumentEvaluator]: ...

_ElementFactory = Callable[[Any, Dict[_AnyStr, _AnyStr]], _Element]
_CommentFactory = Callable[[_AnyStr], _Comment]
_ProcessingInstructionFactory = Callable[[_AnyStr, _AnyStr], _ProcessingInstruction]

class TreeBuilder:
    def __init__(
        self,
        element_factory: Optional[_ElementFactory] = ...,
        parser: Optional[_BaseParser] = ...,
        comment_factory: Optional[_CommentFactory] = ...,
        pi_factory: Optional[_ProcessingInstructionFactory] = ...,
        insert_comments: bool = ...,
        insert_pis: bool = ...,
    ) -> None: ...
    def close(self) -> _Element: ...
    def comment(self, text: _AnyStr) -> None: ...
    def data(self, data: _AnyStr) -> None: ...
    def end(self, tag: _AnyStr) -> None: ...
    def pi(self, target: _AnyStr, data: Optional[_AnyStr] = ...) -> Any: ...
    def start(self, tag: _AnyStr, attrib: Dict[_AnyStr, _AnyStr]) -> None: ...

def iselement(element: Any) -> TypeGuard[_Element]: ...

_ParseEventType: TypeAlias = Literal[
    "start", "end", "start-ns", "end-ns", "comment", "pi"
]
_ParseEvent: TypeAlias = Union[
    tuple[Literal["start"], _Element],
    tuple[Literal["end"], _Element],
    tuple[Literal["start-ns"], Tuple[_AnyStr, _AnyStr]],
    tuple[Literal["end-ns"], None],
    tuple[Literal["comment"], _Comment],
    tuple[Literal["pi"], _ProcessingInstruction],
]

class XMLPullParser(XMLParser):
    def __init__(
        self,
        events: Optional[Iterable[_ParseEventType]] = ...,
        *,
        tag: Optional[_TagSelector] = ...,
        base_url: Optional[_AnyStr] = ...,
        encoding: Optional[_AnyStr] = ...,
        attribute_defaults: bool = ...,
        dtd_validation: bool = ...,
        load_dtd: bool = ...,
        no_network: bool = ...,
        ns_clean: bool = ...,
        recover: bool = ...,
        schema: Optional[XMLSchema] = ...,
        huge_tree: bool = ...,
        remove_blank_text: bool = ...,
        resolve_entities: bool = ...,
        remove_comments: bool = ...,
        remove_pis: bool = ...,
        strip_cdata: bool = ...,
        collect_ids: bool = ...,
        target: Optional[ParserTarget] = ...,
        compact: bool = ...,
    ) -> None: ...
    def read_events(self) -> Iterator[_ParseEvent]: ...

class HTMLPullParser(HTMLParser):
    def __init__(
        self,
        events: Optional[Iterable[_ParseEventType]] = ...,
        *,
        tag: Optional[_TagSelector] = ...,
        base_url: Optional[_AnyStr] = ...,
        encoding: Optional[_AnyStr] = ...,
        collect_ids: bool = ...,
        compact: bool = ...,
        huge_tree: bool = ...,
        no_network: bool = ...,
        recover: bool = ...,
        remove_blank_text: bool = ...,
        remove_comments: bool = ...,
        remove_pis: bool = ...,
        schema: Optional[XMLSchema] = ...,
        strip_cdata: bool = ...,
        target: Optional[ParserTarget] = ...,
    ) -> None: ...
    def read_events(self) -> Iterator[_ParseEvent]: ...
