# mypy: disallow_untyped_defs=False
"""
RDFa extractor

Based on pyrdfa3 and rdflib
"""
import json
import logging
import re
from collections import defaultdict

rdflib_logger = logging.getLogger("rdflib")
rdflib_logger.setLevel(logging.ERROR)

from pyRdfa import Options
from pyRdfa import pyRdfa as PyRdfa
from pyRdfa.initialcontext import initial_context
from rdflib import Graph
from rdflib import logger as rdflib_logger  # type: ignore[no-redef]

from extruct.utils import parse_xmldom_html

# silence rdflib INFO logs
rdflib_logger.setLevel(logging.ERROR)

initial_context["http://www.w3.org/2011/rdfa-context/rdfa-1.1"].ns.update(
    {
        "twitter": "https://dev.twitter.com/cards#",
        "fb": "http://ogp.me/ns/fb#",
        "og": "http://ogp.me/ns#",
        "music": "http://ogp.me/ns/music#",
        "video": "http://ogp.me/ns/video#",
        "article": "http://ogp.me/ns/article#",
        "book": "http://ogp.me/ns/book#",
        "profile": "http://ogp.me/ns/profile#",
    }
)


class RDFaExtractor:
    def _replaceNS(self, prop, html_element, head_element):
        """Expand namespace to match with returned json (e.g.: og -> 'http://ogp.me/ns#')"""

        # context namespaces taken from pyrdfa3
        # https://github.com/RDFLib/PyRDFa/blob/master/pyRdfa/initialcontext.py
        context = {
            "owl": "http://www.w3.org/2002/07/owl#",
            "gr": "http://purl.org/goodrelations/v1#",
            "ctag": "http://commontag.org/ns#",
            "cc": "http://creativecommons.org/ns#",
            "grddl": "http://www.w3.org/2003/g/data-view#",
            "rif": "http://www.w3.org/2007/rif#",
            "sioc": "http://rdfs.org/sioc/ns#",
            "skos": "http://www.w3.org/2004/02/skos/core#",
            "xml": "http://www.w3.org/XML/1998/namespace",
            "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
            "rev": "http://purl.org/stuff/rev#",
            "rdfa": "http://www.w3.org/ns/rdfa#",
            "dc": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "void": "http://rdfs.org/ns/void#",
            "ical": "http://www.w3.org/2002/12/cal/icaltzd#",
            "vcard": "http://www.w3.org/2006/vcard/ns#",
            "wdrs": "http://www.w3.org/2007/05/powder-s#",
            "og": "http://ogp.me/ns#",
            "wdr": "http://www.w3.org/2007/05/powder#",
            "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
            "xhv": "http://www.w3.org/1999/xhtml/vocab#",
            "xsd": "http://www.w3.org/2001/XMLSchema#",
            "v": "http://rdf.data-vocabulary.org/#",
            "skosxl": "http://www.w3.org/2008/05/skos-xl#",
            "schema": "http://schema.org/",
        }

        # if bad property
        if ":" not in prop:
            return prop

        # if property has no prefix
        if "http://" in prop:
            return prop

        prefix = prop.split(":")[0]

        match = None
        if head_element.get("prefix"):
            match = re.search(prefix + r": [^\s]+", head_element.get("prefix"))

        # if namespace taken from prefix attribute in head tag
        if match:
            ns = match.group().split(": ")[1]
            return ns + prop.split(":")[1]

        # if namespace taken from xmlns attribute in html tag
        if ("xmlns:" + prefix) in html_element.keys():
            return html_element.get("xmlns:" + prefix) + prop.split(":")[1]

        # if namespace present in initial context
        if prefix in context:
            return context[prefix] + prop.split(":")[1]

        return prop

    def _sort(self, unordered, ordered):
        """Sort the rdfa tags in jsonld string"""
        idx_for_value = dict(
            reversed([(value, idx) for idx, value in enumerate(ordered)])
        )
        unordered.sort(
            key=lambda props: idx_for_value.get(props.get("@value"), len(ordered))
        )

    def _fix_order(self, jsonld_string, document):
        """
        Fix order of rdfa tags in jsonld string
        by checking the appearance order in the HTML
        """
        json_objects = json.loads(jsonld_string)

        html, head = document.xpath("/html"), document.xpath("//head")
        if not html or not head:
            return json_objects
        html_element, head_element = html[0], head[0]

        # Stores the values or each property in appearance order
        values_for_property = defaultdict(list)

        for meta_tag in head_element.xpath("meta[@property]"):
            expanded_property = self._replaceNS(
                meta_tag.attrib["property"], html_element, head_element
            )
            values_for_property[expanded_property].append(meta_tag.get("content"))

        for json_object in json_objects:
            keys = json_object.keys()

            for key in keys:
                if type(json_object[key]) is list and len(json_object[key]) > 1:
                    self._sort(json_object[key], values_for_property[key])

        return json_objects

    def extract(self, htmlstring, base_url=None, encoding="UTF-8", expanded=True):
        tree = parse_xmldom_html(htmlstring, encoding=encoding)
        return self.extract_items(tree, base_url=base_url, expanded=expanded)

    def extract_items(self, document, base_url=None, expanded=True):
        options = Options(
            output_processor_graph=True,
            embedded_rdf=False,
            space_preserve=True,
            vocab_expansion=False,
            vocab_cache=False,
            vocab_cache_report=False,
            refresh_vocab_cache=False,
            check_lite=False,
        )
        g = PyRdfa(options, base=base_url).graph_from_DOM(
            document, graph=Graph(), pgraph=Graph()
        )
        jsonld_string = g.serialize(format="json-ld", auto_compact=not expanded)

        # rdflib may return either bytes or strings
        if isinstance(jsonld_string, bytes):
            jsonld_string = jsonld_string.decode("utf-8")

        try:
            # hack to fix the ordering of multi-value properties (see issue 116)
            # it should be disabled once PyRDFA fixes itself
            return self._fix_order(jsonld_string, document)
        except:
            return json.loads(jsonld_string)
