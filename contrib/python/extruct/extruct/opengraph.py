# mypy: disallow_untyped_defs=False
import re

from extruct.utils import parse_html

_PREFIX_PATTERN = re.compile(r"\s*(\w+):\s*([^\s]+)")
_OG_NAMESPACES = {
    "og": "http://ogp.me/ns#",
    "music": "http://ogp.me/ns/music#",
    "video": "http://ogp.me/ns/video#",
    "article": "http://ogp.me/ns/article#",
    "book": "http://ogp.me/ns/book#",
    "profile": "http://ogp.me/ns/profile#",
    # non-standard but seen in the wild
    "product": "http://ogp.me/ns/product#",  # ~10% of product pages with OG
}


class OpenGraphExtractor:
    """OpenGraph extractor following extruct API."""

    def extract(self, htmlstring, base_url=None, encoding="UTF-8"):
        tree = parse_html(htmlstring, encoding=encoding)
        return list(self.extract_items(tree, base_url=base_url))

    def extract_items(self, document, base_url=None):
        # OpenGraph defines a web page as a single rich object.
        for head in document.xpath("//head"):
            html_elems = document.head.xpath("parent::html")
            namespaces = self.get_namespaces(html_elems[0]) if html_elems else {}
            namespaces.update(self.get_namespaces(head))
            props = []
            for el in head.xpath("meta[@property and @content]"):
                prop = el.attrib["property"]
                val = el.attrib["content"]
                ns = prop.partition(":")[0]
                if ns in _OG_NAMESPACES:
                    namespaces[ns] = _OG_NAMESPACES[ns]
                if ns in namespaces:
                    props.append((prop, val))
            if props:
                yield {"namespace": namespaces, "properties": props}

    def get_namespaces(self, element):
        return dict(_PREFIX_PATTERN.findall(element.attrib.get("prefix", "")))
