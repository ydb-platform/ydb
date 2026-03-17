from typing import Any, Dict, Optional, Union

from lxml import etree

# dummy for missing stubs
def __getattr__(name) -> Any: ...

_DictAnyStr = Union[Dict[str, str], Dict[bytes, bytes]]

class CSSSelector(etree.XPath):
    def __init__(
        self,
        css: str,
        namespaces: Optional[etree._NonDefaultNSMapArg] = ...,
        translator: str = ...,
    ) -> None: ...
    def __call__(
        self,
        _etree_or_element: Union[etree._Element, etree._ElementTree],
        **_variables: etree._XPathObject
    ) -> etree._XPathObject: ...
