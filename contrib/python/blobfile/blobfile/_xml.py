"""
Slightly faster replacement for xmltodict
"""

from typing import Any

from lxml import etree

Element = etree._Element  # type: ignore


def parse(document: bytes, repeated_tags: set[str] | None = None) -> dict[str, Any]:
    """
    Convert an XML document to a dictionary, like xmltodict.parse but with lxml
    """
    if repeated_tags is None:
        repeated_tags = set()
    root = etree.fromstring(document)
    children = _recursive_dict(root, repeated_tags=repeated_tags)
    return {root.tag: children}


def _recursive_dict(elem: Element, repeated_tags: set[str]) -> dict[str, Any] | str | None:
    if len(elem) == 0:
        return elem.text
    else:
        seen_tags = set()
        result = {}
        for child in elem:
            if child.tag in repeated_tags:
                child_list = result.setdefault(child.tag, [])
                child_list.append(_recursive_dict(child, repeated_tags))
            else:
                assert (
                    child.tag not in seen_tags
                ), f"Found repeated tag that was not in repeated_tags set: {child.tag}"
                seen_tags.add(child.tag)
                result[child.tag] = _recursive_dict(child, repeated_tags)
        return result


def unparse(data: dict[str, Any]) -> bytes:
    """
    Convert a dictionary into an XML document, like xmltodict.unparse but with lxml
    """
    root_keys = list(data.keys())
    assert len(root_keys) == 1, f"Must be only one root element, found {root_keys}"
    root = _create_tree(root_keys[0], data[root_keys[0]])
    # xml_declaration=True doesn't work for some reason, it seems
    # to use single quotes that azure doesn't like
    return b'<?xml version="1.0" encoding="utf-8"?>\n' + etree.tostring(root, encoding="utf8")


def _create_tree(name: str, data: dict[str, Any]) -> Element:
    elem = etree.Element(name)
    for k, v in data.items():
        if isinstance(v, dict):
            elem.append(_create_tree(k, v))
        elif isinstance(v, list):
            for item in v:
                if isinstance(item, str):
                    se = etree.Element(k)
                    se.text = item
                    elem.append(se)
                elif isinstance(item, dict):
                    elem.append(_create_tree(k, item))
                else:
                    assert False, f"Invalid value item type: {type(item)}"
        else:
            assert isinstance(v, str) or v is None
            se = etree.Element(k)
            if v is None:
                # match xmltodict's behavior and use <Elem></Elem> instead of <Elem />
                se.text = ""
            else:
                se.text = v
            elem.append(se)
    return elem
