import copy
import json
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup, FeatureNotFound
from bs4.element import Tag

from . import (
    backcompat,
    implied_properties,
    metaformats,
    mf2_classes,
    parse_property,
    temp_fixes,
)
from .dom_helpers import get_attr, get_children, get_descendents, try_urljoin
from .mf_helpers import unordered_list
from .version import __version__


def parse(
    doc=None,
    url=None,
    html_parser=None,
    expose_dom=False,
    metaformats=False,
    filter_roots=False,
):
    """
    Parse a document or URL for microformats and return a dictionary in mf2json format.

    Args:
      doc (file, string or BeautifulSoup doc): file handle, text of content
        to parse, or BeautifulSoup document. If None it will be fetched from
        given URL.
      url (string): URL of the file to be processed. If None it will be
        extracted from the `<base>` element of given doc.
      html_parser (string): optional, select a specific HTML parser. Valid options
        from the BeautifulSoup documentation are: "html", "xml","html5", "lxml",
        "html5lib", and "html.parser".
      expose_dom (boolean): optional, expose the DOM of embedded properties.
      metaformats (boolean): optional, include metaformats extracted from OGP
        and Twitter card data: https://microformats.org/wiki/metaformats
      filter_roots (boolean or list): optional, filter root class names. Use
        True to filter known conflicting classes, otherwise filter given list.

    Return: a mf2json dict representing the structured data in the document

    """
    return Parser(
        doc,
        url,
        html_parser,
        expose_dom=expose_dom,
        metaformats=metaformats,
        filter_roots=filter_roots,
    ).to_dict()


class Parser(object):
    """
    Parser to parse a document or URL for microformats and output in various formats.

    Args:
      doc (file, string or BeautifulSoup doc): file handle, text of content
        to parse, or BeautifulSoup document. If None it will be fetched from
        given URL.
      url (string): URL of the file to be processed. If None it will be
        extracted from the `<base>` element of given doc.
      html_parser (string): optional, select a specific HTML parser. Valid options
        from the BeautifulSoup documentation are: "html", "xml","html5", "lxml",
        "html5lib", and "html.parser".
      expose_dom (boolean): optional, expose the DOM of embedded properties.
      metaformats (boolean): optional, include metaformats extracted from OGP
        and Twitter card data: https://microformats.org/wiki/metaformats
      filter_roots (boolean or list): optional, filter root class names. Use
        True to filter known conflicting classes, otherwise filter given list.

    Attributes:
      useragent (string): the User-Agent string for the Parser

    """

    ua_desc = "mf2py - microformats2 parser for python"
    ua_url = "https://github.com/microformats/mf2py"
    useragent = "{0} - version {1} - {2}".format(ua_desc, __version__, ua_url)

    def __init__(
        self,
        doc=None,
        url=None,
        html_parser=None,
        expose_dom=False,
        metaformats=False,
        filter_roots=False,
    ):
        self.__url__ = None
        self.__doc__ = None
        self._preserve_doc = False
        self.__parsed__ = {
            "items": [],
            "rels": {},
            "rel-urls": {},
            "debug": {
                "description": self.ua_desc,
                "source": self.ua_url,
                "version": __version__,
            },
        }
        self.lang = None
        self.expose_dom = expose_dom
        self.__metaformats = metaformats
        try:
            self.filtered_roots = set(filter_roots)
        except TypeError:
            if filter_roots:
                self.filtered_roots = mf2_classes.CONFLICTING_ROOTS_TAILWIND
            else:
                self.filtered_roots = []

        # use default parser if none specified
        self.__html_parser__ = html_parser or "html5lib"

        if url is not None:
            self.__url__ = url

            if doc is None:
                data = requests.get(
                    self.__url__,
                    headers={
                        "User-Agent": self.useragent,
                    },
                )

                # update to final URL after redirects
                self.__url__ = data.url

                # HACK: check for character encodings and use 'correct' data
                if "charset" in data.headers.get("content-type", ""):
                    doc = data.text
                else:
                    doc = data.content

        if doc is not None:
            if isinstance(doc, BeautifulSoup) or isinstance(doc, Tag):
                self.__doc__ = doc
                self._preserve_doc = True
            else:
                try:
                    # try the user-given html parser or default html5lib
                    self.__doc__ = BeautifulSoup(doc, features=self.__html_parser__)
                except FeatureNotFound:
                    # maybe raise a warning?
                    # else switch to default use
                    self.__doc__ = BeautifulSoup(doc)

        # update actual parser used
        # uses builder.NAME from BeautifulSoup
        if isinstance(self.__doc__, BeautifulSoup) and self.__doc__.builder is not None:
            self.__html_parser__ = self.__doc__.builder.NAME
        else:
            self.__html_parser__ = None

        # check for <base> tag
        if self.__doc__:
            poss_base = next(
                (el for el in get_descendents(self.__doc__) if el.name == "base"), None
            )
            if poss_base:
                poss_base_url = poss_base.get("href")  # try to get href
                if poss_base_url:
                    if urlparse(poss_base_url).netloc:
                        # base specifies an absolute path
                        self.__url__ = poss_base_url
                    elif self.__url__:
                        # base specifies a relative path
                        self.__url__ = try_urljoin(self.__url__, poss_base_url)

        if self.__doc__ is not None:
            if document := self.__doc__.find("html"):
                self.lang = document.attrs.get("lang")
            # parse!
            self._parse()

    def _parse(self):
        """Does the work of actually parsing the document. Done automatically
        on initialization.
        """
        self._default_date = None
        # _default_date exists to provide implementation for rules described
        # in legacy value-class-pattern. basically, if you have two dt-
        # properties and one does not have the full date, it can use the
        # existing date as a template.
        # see value-class-pattern#microformats2_parsers on wiki.
        # see also the implied_relative_datetimes testcase.

        def handle_microformat(
            root_class_names,
            el,
            value_property=None,
            simple_value=None,
            backcompat_mode=False,
        ):
            """Handles a (possibly nested) microformat, i.e. h-*"""
            properties = {}
            children = []
            self._default_date = None
            # for processing implied properties: collects if property types (p, e, u, d(t)) or children (h) have been processed
            parsed_types_aggregation = set()

            if backcompat_mode:
                el = backcompat.apply_rules(
                    el, self.__html_parser__, self.filtered_roots
                )
                root_class_names = mf2_classes.root(
                    el.get("class", []), self.filtered_roots
                )

            root_lang = el.attrs.get("lang")

            # parse for properties and children
            for child in get_children(el):
                (
                    child_props,
                    child_children,
                    child_parsed_types_aggregation,
                ) = parse_props(child, root_lang)
                for key, new_value in child_props.items():
                    prop_value = properties.get(key, [])
                    prop_value.extend(new_value)
                    properties[key] = prop_value
                children.extend(child_children)
                parsed_types_aggregation.update(child_parsed_types_aggregation)

            # complex h-* objects can take their "value" from the
            # first explicit property ("name" for p-* or "url" for u-*)
            if value_property and value_property in properties:
                simple_value = properties[value_property][0]

            # if some properties not already found find in implied ways unless in backcompat mode
            if not backcompat_mode:
                # stop implied name if any p-*, e-*, h-* is already found
                if "name" not in properties and parsed_types_aggregation.isdisjoint(
                    "peh"
                ):
                    properties["name"] = [
                        implied_properties.name(el, self.__url__, self.filtered_roots)
                    ]

                if "photo" not in properties and parsed_types_aggregation.isdisjoint(
                    "uh"
                ):
                    x = implied_properties.photo(el, self.__url__, self.filtered_roots)
                    if x is not None:
                        properties["photo"] = [x]

                # stop implied url if any u-* or h-* is already found
                if "url" not in properties and parsed_types_aggregation.isdisjoint(
                    "uh"
                ):
                    x = implied_properties.url(el, self.__url__, self.filtered_roots)
                    if x is not None:
                        properties["url"] = [x]

            # build microformat with type and properties
            microformat = {
                "type": [class_name for class_name in sorted(root_class_names)],
                "properties": properties,
            }
            if el.name == "area":
                shape = get_attr(el, "shape")
                if shape is not None:
                    microformat["shape"] = shape

                coords = get_attr(el, "coords")
                if coords is not None:
                    microformat["coords"] = coords

            # insert children if any
            if children:
                microformat["children"] = children

            Id = get_attr(el, "id")
            if Id:
                microformat["id"] = Id

            # simple value is the parsed property value if it were not
            # an h-* class
            if simple_value is not None:
                if isinstance(simple_value, dict):
                    # for e-* properties, the simple value will be
                    # {"html":..., "value":...}  which we should fold
                    # into the microformat object
                    # details: https://github.com/microformats/mf2py/issues/35
                    microformat.update(simple_value)
                else:
                    microformat["value"] = simple_value

            if root_lang:
                microformat["lang"] = root_lang
            elif self.lang:
                microformat["lang"] = self.lang
            return microformat

        def parse_props(el, root_lang):
            """Parse the properties from a single element"""
            props = {}
            children = []
            # for processing implied properties: collects if property types (p, e, u, d(t)) or children (h) have been processed
            parsed_types_aggregation = set()

            classes = el.get("class", [])
            filtered_classes = mf2_classes.filter_classes(classes)
            # Is this element a microformat2 root?
            root_class_names = filtered_classes["h"]
            backcompat_mode = False

            # Is this element a microformat1 root?
            if not root_class_names:
                root_class_names = backcompat.root(classes)
                backcompat_mode = True

            if root_class_names:
                parsed_types_aggregation.add("h")

            # Is this a property element (p-*, u-*, etc.) flag
            # False is default
            is_property_el = False

            # Parse plaintext p-* properties.
            p_value = None
            for prop_name in filtered_classes["p"]:
                is_property_el = True
                parsed_types_aggregation.add("p")
                prop_value = props.setdefault(prop_name, [])

                # if value has not been parsed then parse it
                if p_value is None:
                    p_value = parse_property.text(el, base_url=self.__url__)

                if root_class_names:
                    prop_value.append(
                        handle_microformat(
                            root_class_names,
                            el,
                            value_property="name",
                            simple_value=p_value,
                            backcompat_mode=backcompat_mode,
                        )
                    )
                else:
                    prop_value.append(p_value)

            # Parse URL u-* properties.
            u_value = None
            for prop_name in filtered_classes["u"]:
                is_property_el = True
                parsed_types_aggregation.add("u")
                prop_value = props.setdefault(prop_name, [])

                # if value has not been parsed then parse it
                if u_value is None:
                    u_value = parse_property.url(el, base_url=self.__url__)

                if root_class_names:
                    prop_value.append(
                        handle_microformat(
                            root_class_names,
                            el,
                            value_property="url",
                            simple_value=u_value,
                            backcompat_mode=backcompat_mode,
                        )
                    )
                else:
                    if isinstance(u_value, dict):
                        prop_value.append(u_value)
                    else:
                        prop_value.append(u_value)

            # Parse datetime dt-* properties.
            dt_value = None
            for prop_name in filtered_classes["dt"]:
                is_property_el = True
                parsed_types_aggregation.add("d")
                prop_value = props.setdefault(prop_name, [])

                # if value has not been parsed then parse it
                if dt_value is None:
                    dt_value, new_date = parse_property.datetime(el, self._default_date)
                    # update the default date
                    if new_date:
                        self._default_date = new_date

                if root_class_names:
                    stops_implied_name = True
                    prop_value.append(
                        handle_microformat(
                            root_class_names,
                            el,
                            simple_value=dt_value,
                            backcompat_mode=backcompat_mode,
                        )
                    )
                else:
                    if dt_value is not None:
                        prop_value.append(dt_value)

            # Parse embedded markup e-* properties.
            e_value = None
            for prop_name in filtered_classes["e"]:
                is_property_el = True
                parsed_types_aggregation.add("e")
                prop_value = props.setdefault(prop_name, [])

                # if value has not been parsed then parse it
                if e_value is None:
                    # send original element for parsing backcompat
                    if el.original is None:
                        embedded_el = el
                    else:
                        embedded_el = el.original
                    if self._preserve_doc:
                        embedded_el = copy.copy(embedded_el)
                    temp_fixes.rm_templates(embedded_el)
                    e_value = parse_property.embedded(
                        embedded_el, self.__url__, root_lang, self.lang, self.expose_dom
                    )

                if root_class_names:
                    stops_implied_name = True
                    prop_value.append(
                        handle_microformat(
                            root_class_names,
                            el,
                            simple_value=e_value,
                            backcompat_mode=backcompat_mode,
                        )
                    )
                else:
                    prop_value.append(e_value)

            # if this is not a property element, but it is a h-* microformat,
            # add it to our list of children
            if not is_property_el and root_class_names:
                children.append(
                    handle_microformat(
                        root_class_names, el, backcompat_mode=backcompat_mode
                    )
                )
            # parse child tags, provided this isn't a microformat root-class
            if not root_class_names:
                for child in get_children(el):
                    (
                        child_properties,
                        child_microformats,
                        child_parsed_types_aggregation,
                    ) = parse_props(child, root_lang)
                    for prop_name in child_properties:
                        v = props.get(prop_name, [])
                        v.extend(child_properties[prop_name])
                        props[prop_name] = v
                    children.extend(child_microformats)
                    parsed_types_aggregation.update(child_parsed_types_aggregation)
            return props, children, parsed_types_aggregation

        def parse_rels(el):
            """Parse an element for rel microformats"""
            rel_attrs = get_attr(el, "rel")
            # if rel attributes exist
            if rel_attrs is not None:
                # find the url and normalise it
                url = try_urljoin(self.__url__, el.get("href", ""))
                value_dict = self.__parsed__["rel-urls"].get(url, {})

                # 1st one wins
                if "text" not in value_dict:
                    value_dict["text"] = el.get_text().strip()

                url_rels = value_dict.get("rels", [])
                value_dict["rels"] = url_rels

                for knownattr in ("media", "hreflang", "type", "title"):
                    x = get_attr(el, knownattr)
                    # 1st one wins
                    if x is not None and knownattr not in value_dict:
                        value_dict[knownattr] = x

                self.__parsed__["rel-urls"][url] = value_dict

                for rel_value in rel_attrs:
                    value_list = self.__parsed__["rels"].get(rel_value, [])
                    if url not in value_list:
                        value_list.append(url)
                    if rel_value not in url_rels:
                        url_rels.append(rel_value)

                    self.__parsed__["rels"][rel_value] = value_list
                if "alternate" in rel_attrs:
                    alternate_list = self.__parsed__.get("alternates", [])
                    alternate_dict = {}
                    alternate_dict["url"] = url
                    x = " ".join([r for r in rel_attrs if not r == "alternate"])
                    if x != "":
                        alternate_dict["rel"] = x
                    alternate_dict["text"] = el.get_text().strip()
                    for knownattr in ("media", "hreflang", "type", "title"):
                        x = get_attr(el, knownattr)
                        if x is not None:
                            alternate_dict[knownattr] = x
                    alternate_list.append(alternate_dict)
                    self.__parsed__["alternates"] = alternate_list

        def parse_el(el, ctx):
            """Parse an element for microformats"""
            classes = el.get("class", [])

            # find potential microformats in root classnames h-*
            potential_microformats = mf2_classes.root(classes, self.filtered_roots)

            # if potential microformats found parse them
            if potential_microformats:
                result = handle_microformat(potential_microformats, el)
                ctx.append(result)
            else:
                # find backcompat root classnames
                potential_microformats = backcompat.root(classes)
                if potential_microformats:
                    result = handle_microformat(
                        potential_microformats, el, backcompat_mode=True
                    )
                    ctx.append(result)
                else:
                    # parse child tags
                    for child in get_children(el):
                        parse_el(child, ctx)

        ctx = []

        if self.__metaformats:
            # extract out a metaformats item, if available
            self.__metaformats_item = metaformats.parse(self.__doc__, url=self.__url__)

        # start parsing at root element of the document
        parse_el(self.__doc__, ctx)
        self.__parsed__["items"] = ctx
        if self.__metaformats and self.__metaformats_item:
            self.__parsed__["items"].append(self.__metaformats_item)

        # parse for rel values
        for el in get_descendents(self.__doc__):
            if el.name in ("a", "area", "link") and el.has_attr("rel"):
                parse_rels(el)

        # sort the rels array in rel-urls since this should be unordered set
        for url in self.__parsed__["rel-urls"]:
            if "rels" in self.__parsed__["rel-urls"][url]:
                rels = self.__parsed__["rel-urls"][url]["rels"]
                self.__parsed__["rel-urls"][url]["rels"] = unordered_list(rels)

        # add actual parser used to debug
        # uses builder.NAME from BeautifulSoup
        if self.__html_parser__:
            self.__parsed__["debug"]["markup parser"] = self.__html_parser__
        else:
            self.__parsed__["debug"]["markup parser"] = "unknown"

    def to_dict(self, filter_by_type=None):
        """Get a dictionary version of the parsed microformat document.

        Args:
          filter_by_type (string, optional): only include top-level items of
            the given h-* type. Defaults to None.

        Returns:
            dict: representation of the parsed microformats document
        """
        if filter_by_type is None:
            return self.__parsed__
        else:
            return [x for x in self.__parsed__["items"] if filter_by_type in x["type"]]

    def to_json(self, pretty_print=False, filter_by_type=None):
        """Get a json-encoding string version of the parsed microformats document

        Args:
          pretty_print (bool, optional): Encode the json document with
            linebreaks and indents to improve readability. Defaults to False.
          filter_by_type (bool, optional): only include top-level items of
            the given h-* type

        Returns:
            string: a json-encoded string
        """

        if pretty_print:
            return json.dumps(
                self.to_dict(filter_by_type), indent=4, separators=(", ", ": ")
            )
        else:
            return json.dumps(self.to_dict(filter_by_type))
