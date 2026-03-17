from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Iterator,
    List,
    MutableMapping,
    MutableSet,
    Optional,
    Tuple,
)

from typing_extensions import Literal

if TYPE_CHECKING:
    from ..etree import HTMLParser as _HTMLParser
    from ..etree import XMLParser as _XMLParser
    from ..etree import _AnySmartStr, _AnyStr, _BaseParser, _Element

_HANDLE_FALURES = Literal["ignore", "discard", None]

XHTML_NAMESPACE: str = ...

class Classes(MutableSet):
    def __init__(self, attributes: MutableMapping[str, Any]) -> None: ...
    def add(self, value: str) -> None: ...
    def discard(self, value: str) -> None: ...
    def remove(self, value: str) -> None: ...
    def __contains__(self, name: object) -> bool: ...
    def __iter__(self) -> Iterator[str]: ...
    def __len__(self) -> int: ...
    def update(self, values: Iterable[str]) -> None: ...
    def toggle(self, value: str) -> bool: ...

class HtmlMixin:
    base_url: Optional[str]
    forms: Optional[List["_Element"]]
    body: Optional["_Element"]
    head: Optional["_Element"]
    label: Optional["_Element"]
    def set(self, key: str, value: Any) -> None: ...
    def drop_tree(self) -> None: ...
    def drop_tag(self) -> None: ...
    def find_rel_links(self, rel: str) -> List["_Element"]: ...
    def find_class(self, class_name: str) -> List["_Element"]: ...
    def get_element_by_id(self, id: str, *default) -> Optional["_Element"]: ...
    def text_content(self) -> "_AnySmartStr": ...
    def cssselect(self, expr: str, translator: str = ...) -> List["_Element"]: ...
    def make_links_absolute(
        self,
        base_url: str = ...,
        resolve_base_href: bool = ...,
        handle_failures: _HANDLE_FALURES = ...,
    ) -> str: ...
    def resolve_base_href(self, handle_failures: _HANDLE_FALURES = ...) -> None: ...
    def iterlinks(self) -> Iterator[Tuple["_Element", Optional[str], str, int]]: ...
    def rewrite_links(
        self,
        link_repl_func: Callable[[str], Optional[str]],
        resolve_base_href: bool = ...,
        base_href: str = ...,
    ) -> None: ...
    def __getattr__(self, name: str) -> Any: ...  # incomplete

class HTMLParser(_HTMLParser):
    pass

class XHTMLParser(_XMLParser):
    pass

def document_fromstring(
    html: "_AnyStr", parser: "_BaseParser" = ..., ensure_head_body: bool = ..., **kw
) -> "_Element": ...
def fragments_fromstring(
    html: "_AnyStr",
    no_leading_text: bool = ...,
    base_url: str = ...,
    parser: "_BaseParser" = ...,
    **kw
) -> "_Element": ...
def fromstring(
    html: "_AnyStr", base_url: str = ..., parser: "_BaseParser" = ..., **kw
) -> "_Element": ...
def __getattr__(name: str) -> Any: ...  # incomplete
