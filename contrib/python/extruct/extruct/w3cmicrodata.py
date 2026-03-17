# mypy: disallow_untyped_defs=False
"""
HTML Microdata parser

Piece of code extracted form:
* http://blog.scrapinghub.com/2014/06/18/extracting-schema-org-microdata-using-scrapy-selectors-and-xpath/

Ported to lxml
follows http://www.w3.org/TR/microdata/#json

"""

from __future__ import annotations

import collections
from functools import partial
from typing import Any, Set
from urllib.parse import urljoin

import html_text
import lxml.etree
from lxml.html.clean import Cleaner
from w3lib.html import strip_html5_whitespace

from extruct.utils import parse_html

# Cleaner which is similar to html_text cleaner, but is less aggressive
cleaner = Cleaner(
    scripts=True,
    javascript=False,  # onclick attributes are fine
    comments=True,
    style=True,
    links=True,
    meta=True,
    page_structure=False,  # <title> may be nice to have
    processing_instructions=True,
    embedded=False,  # keep embedded content
    frames=False,  # keep frames
    forms=False,  # keep forms
    annoying_tags=False,
    remove_unknown_tags=False,
    safe_attrs_only=False,
)


class LxmlMicrodataExtractor:
    # iterate in document order (used below for fast get_docid)
    _xp_item = lxml.etree.XPath("descendant-or-self::*[@itemscope]")
    _xp_prop = lxml.etree.XPath(
        """set:difference(
            .//*[@itemprop],
            .//*[@itemscope]//*[@itemprop])""",
        namespaces={"set": "http://exslt.org/sets"},
    )
    _xp_clean_text = lxml.etree.XPath(
        "descendant-or-self::*[not(self::script or self::style)]/text()"
    )

    def __init__(
        self, nested=True, strict=False, add_text_content=False, add_html_node=False
    ):
        self.nested = nested
        self.strict = strict
        self.add_text_content = add_text_content
        self.add_html_node = add_html_node

    def extract(self, htmlstring, base_url=None, encoding="UTF-8"):
        tree = parse_html(htmlstring, encoding=encoding)
        return self.extract_items(tree, base_url)

    def extract_items(self, document, base_url):
        itemids = self._build_itemids(document)
        items_seen: set[Any] = set()
        return [
            item
            for item in (
                self._extract_item(
                    it, items_seen=items_seen, base_url=base_url, itemids=itemids
                )
                for it in self._xp_item(document)  # type: ignore[union-attr]
            )
            if item
        ]

    def get_docid(self, node, itemids):
        return itemids[node]

    def _build_itemids(self, document):
        """Build itemids for a fast get_docid implementation. Use document order."""
        root = document.getroottree().getroot()
        return {node: idx + 1 for idx, node in enumerate(self._xp_item(root))}  # type: ignore[arg-type]

    def _extract_item(self, node, items_seen, base_url, itemids):
        itemid = self.get_docid(node, itemids)

        if self.nested:
            if itemid in items_seen:
                return
            items_seen.add(itemid)

        item = {}
        if not self.nested:
            item["iid"] = itemid
        types = node.get("itemtype", "").split()
        if types:
            if not self.strict and len(types) == 1:
                item["type"] = types[0]
            else:
                item["type"] = types

            nodeid = node.get("itemid")
            if nodeid:
                item["id"] = nodeid.strip()

        properties = collections.defaultdict(list)
        for name, value in self._extract_properties(
            node, items_seen=items_seen, base_url=base_url, itemids=itemids
        ):
            properties[name].append(value)

        # process item references
        refs = node.get("itemref", "").split()
        if refs:
            for refid in refs:
                for name, value in self._extract_property_refs(
                    node,
                    refid,
                    items_seen=items_seen,
                    base_url=base_url,
                    itemids=itemids,
                ):
                    properties[name].append(value)

        props = []
        for name, values in properties.items():
            if not self.strict and len(values) == 1:
                props.append((name, values[0]))
            else:
                props.append((name, values))
        if props:
            item["properties"] = dict(props)
        else:
            # item without properties; let's use the node itself
            item["value"] = self._extract_property_value(
                node,
                force=True,
                items_seen=items_seen,
                base_url=base_url,
                itemids=itemids,
            )

        # below are not in the specs, but can be handy
        if self.add_text_content:
            textContent = self._extract_textContent(node)
            if textContent:
                item["textContent"] = textContent
        if self.add_html_node:
            item["htmlNode"] = node

        return item

    def _extract_properties(self, node, items_seen, base_url, itemids):
        for prop in self._xp_prop(node):  # type: ignore[union-attr]
            yield from self._extract_property(
                prop, items_seen=items_seen, base_url=base_url, itemids=itemids
            )

    def _extract_property_refs(self, node, refid, items_seen, base_url, itemids):
        ref_node = node.xpath("id($refid)[1]", refid=refid)
        if not ref_node:
            return
        ref_node = ref_node[0]
        extract_fn = partial(
            self._extract_property,
            items_seen=items_seen,
            base_url=base_url,
            itemids=itemids,
        )
        if "itemprop" in ref_node.keys() and "itemscope" in ref_node.keys():
            # An full item will be extracted from the node, no need to look
            # for individual properties in child nodes
            yield from extract_fn(ref_node)
        else:
            base_parent_scope = ref_node.xpath("ancestor-or-self::*[@itemscope][1]")
            for prop in ref_node.xpath("descendant-or-self::*[@itemprop]"):
                parent_scope = prop.xpath("ancestor::*[@itemscope][1]")
                # Skip properties defined in a different scope than the ref_node
                if parent_scope == base_parent_scope:
                    yield from extract_fn(prop)

    def _extract_property(self, node, items_seen, base_url, itemids):
        props = node.get("itemprop").split()
        value = self._extract_property_value(
            node, items_seen=items_seen, base_url=base_url, itemids=itemids
        )
        return [(p, value) for p in props]

    def _extract_property_value(self, node, items_seen, base_url, itemids, force=False):
        # http://www.w3.org/TR/microdata/#values
        if not force and node.get("itemscope") is not None:
            if self.nested:
                return self._extract_item(
                    node, items_seen=items_seen, base_url=base_url, itemids=itemids
                )
            else:
                return {"iid_ref": self.get_docid(node, itemids)}

        elif node.tag == "meta":
            return node.get("content", "")

        elif node.tag in (
            "audio",
            "embed",
            "iframe",
            "img",
            "source",
            "track",
            "video",
        ):
            return urljoin(base_url, strip_html5_whitespace(node.get("src", "")))

        elif node.tag in ("a", "area", "link"):
            return urljoin(base_url, strip_html5_whitespace(node.get("href", "")))

        elif node.tag in ("object",):
            return urljoin(base_url, strip_html5_whitespace(node.get("data", "")))

        elif node.tag in ("data", "meter"):
            return node.get("value", "")

        elif node.tag in ("time",):
            return node.get("datetime", "")

        # not in W3C specs but used in schema.org examples
        elif node.get("content"):
            return node.get("content")

        # https://schema.org/docs/actions.html#part-4
        elif (itemprop := node.get("itemprop")) and (
            itemprop.endswith("-input") or itemprop.endswith("-output")
        ):
            result = {}
            if "required" in node.attrib:
                result["valueRequired"] = True
            if name := node.get("name"):
                result["valueName"] = name
            return result

        else:
            return self._extract_textContent(node)

    def _extract_textContent(self, node):
        clean_node = cleaner.clean_html(node)
        return html_text.etree_to_text(clean_node)


MicrodataExtractor = LxmlMicrodataExtractor
