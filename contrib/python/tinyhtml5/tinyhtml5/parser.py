from . import inputstream
from .constants import (
    ReparseError,
    Token,
    adjust_foreign_attributes,
    adjust_mathml_attributes,
    adjust_svg_attributes,
    ascii_upper_to_lower,
    cdata_elements,
    heading_elements,
    html_integration_point_elements,
    mathml_text_integration_point_elements,
    namespaces,
    rcdata_elements,
    space_characters,
    special_elements,
)
from .tokenizer import HTMLTokenizer
from .treebuilder import Marker, TreeBuilder


def parse(document, namespace_html_elements=True, **kwargs):
    """Parse an HTML document into a tree.

    :param document:
        The document to parse as a HTML string, filename, file-like object.
    :type document:
        :class:`str`, :class:`bytes`, :class:`pathlib.Path` or
        :term:`file object`
    :param bool namespace_html_elements:
        Whether or not to namespace HTML elements.

    Extra parameters can be provided to define possible encodings if the
    document is given as :class:`bytes`.

    :param override_encoding: Forced encoding provided by user agent.
    :type override_encoding: str or bytes
    :param transport_encoding: Encoding provided by transport layout.
    :type transport_encoding: str or bytes
    :param same_origin_parent_encoding: Parent document encoding.
    :type same_origin_parent_encoding: str or bytes
    :param likely_encoding: Possible encoding provided by user agent.
    :type likely_encoding: str or bytes
    :param default_encoding: Encoding used as fallback.
    :type default_encoding: str or bytes

    :returns: :class:`xml.etree.ElementTree.Element`.

    Example:

    >>> from tinyhtml5 import parse
    >>> parse('<html><body><p>This is a doc</p></body></html>')
    <Element '{http://www.w3.org/1999/xhtml}html' at …>

    """
    return HTMLParser(namespace_html_elements).parse(document, **kwargs)


class HTMLParser:
    """HTML parser.

    Generate a tree structure from a stream of (possibly malformed) HTML.

    """

    def __init__(self, namespace_html_elements=True):
        self.tree = TreeBuilder(namespace_html_elements)
        self.errors = []
        self.phases = {name: cls(self, self.tree) for name, cls in _phases.items()}

    def _parse(self, stream, container=None, scripting=False, **kwargs):
        self.container = container
        self.scripting = scripting
        self.tokenizer = HTMLTokenizer(stream, parser=self, **kwargs)
        self.reset()
        try:
            self.main_loop()
        except ReparseError:
            self.reset()
            self.main_loop()

    def reset(self):
        self.tree.reset()
        self.first_start_tag = False
        self.errors = []
        self.compatibility_mode = "no quirks"  # or "quirks" or "limited quirks"

        if self.container:
            if self.container in cdata_elements:
                self.tokenizer.state = self.tokenizer.rcdata_state
            elif self.container in rcdata_elements:
                self.tokenizer.state = self.tokenizer.rawtext_state
            elif self.container == 'plaintext':
                self.tokenizer.state = self.tokenizer.plaintext_state
            else:
                # State already is data state.
                # self.tokenizer.state = self.tokenizer.data_state
                pass
            self.phase = self.phases["before html"]
            self.phase._insert_html_element()
            self.reset_insertion_mode()
        else:
            self.phase = self.phases["initial"]

        self.last_phase = None

        self.before_rcdata_phase = None

        self.frameset_ok = True

    @property
    def encoding(self):
        """Name of the character encoding that was used to decode the input stream.

        :obj:`None` if that is not determined yet.

        """
        if hasattr(self, 'tokenizer'):
            return self.tokenizer.stream.encoding[0].name

    def is_html_integration_point(self, element):
        full_name = (element.namespace, element.name)
        if full_name == (namespaces["mathml"], "annotation-xml"):
            return (
                "encoding" in element.attributes and
                element.attributes["encoding"].translate(ascii_upper_to_lower) in
                ("text/html", "application/xhtml+xml"))
        return full_name in html_integration_point_elements

    def is_mathml_text_integration_point(self, element):
        full_name = (element.namespace, element.name)
        return full_name in mathml_text_integration_point_elements

    def main_loop(self):
        for token in self.tokenizer:
            previous_token = None
            new_token = token
            while new_token is not None:
                previous_token = new_token
                current_node = (
                    self.tree.open_elements[-1] if self.tree.open_elements else None)
                current_node_namespace = (
                    current_node.namespace if current_node else None)
                current_node_name = current_node.name if current_node else None

                type = new_token["type"]

                if type == Token.PARSE_ERROR:
                    self.parse_error(new_token["data"], new_token.get("datavars", {}))
                    new_token = None
                else:
                    if (len(self.tree.open_elements) == 0 or
                        current_node_namespace == self.tree.default_namespace or
                        (self.is_mathml_text_integration_point(current_node) and
                         ((type == Token.START_TAG and
                           token["name"] not in frozenset(["mglyph", "malignmark"])) or
                          type in (Token.CHARACTERS, Token.SPACE_CHARACTERS))) or
                        (current_node_namespace == namespaces["mathml"] and
                         current_node_name == "annotation-xml" and
                         type == Token.START_TAG and
                         token["name"] == "svg") or
                        (self.is_html_integration_point(current_node) and type in (
                            Token.START_TAG, Token.CHARACTERS,
                            Token.SPACE_CHARACTERS))):
                        phase = self.phase
                    else:
                        phase = self.phases["in foreign content"]

                    if type == Token.CHARACTERS:
                        new_token = phase.process_characters(new_token)
                    elif type == Token.SPACE_CHARACTERS:
                        new_token = phase.process_space_characters(new_token)
                    elif type == Token.START_TAG:
                        new_token = phase.process_start_tag(new_token)
                    elif type == Token.END_TAG:
                        new_token = phase.process_end_tag(new_token)
                    elif type == Token.COMMENT:
                        new_token = phase.process_comment(new_token)
                    elif type == Token.DOCTYPE:
                        new_token = phase.process_doctype(new_token)

            if (type == Token.START_TAG and previous_token["selfClosing"] and
                    not previous_token["selfClosingAcknowledged"]):
                self.parse_error(
                    "non-void-element-with-trailing-solidus",
                    {"name": previous_token["name"]})

        # When the loop finishes it's EOF.
        reprocess = True
        phases = []
        while reprocess:
            phases.append(self.phase)
            reprocess = self.phase.process_eof()
            if reprocess:
                assert self.phase not in phases

    def parse(self, stream, full_tree=False, **kwargs):
        """Parse a HTML document into a well-formed tree.

        If ``full_tree`` is ``True``, return the whole tree.

        """
        self._parse(stream, **kwargs)
        return self.tree.get_document(full_tree)

    def parse_fragment(self, stream, container="div", **kwargs):
        """Parse a HTML fragment into a well-formed tree fragment.

        ``container`` is the tag name of the fragment’s container.

        """
        self._parse(stream, container=container, **kwargs)
        return self.tree.get_fragment()

    def parse_error(self, errorcode, datavars=None):
        if datavars is None:
            datavars = {}
        self.errors.append((self.tokenizer.stream.position(), errorcode, datavars))

    def adjust_mathml_attributes(self, token):
        adjust_attributes(token, adjust_mathml_attributes)

    def adjust_svg_attributes(self, token):
        adjust_attributes(token, adjust_svg_attributes)

    def adjust_foreign_attributes(self, token):
        adjust_attributes(token, adjust_foreign_attributes)

    def reset_insertion_mode(self):
        # The name of this method is mostly historical. (It's also used in the
        # specification.)
        last = False
        new_modes = {
            "select": "in select",
            "td": "in cell",
            "th": "in cell",
            "tr": "in row",
            "tbody": "in table body",
            "thead": "in table body",
            "tfoot": "in table body",
            "caption": "in caption",
            "colgroup": "in column group",
            "table": "in table",
            "head": "in body",
            "body": "in body",
            "frameset": "in frameset",
            "html": "before head"
        }
        for node in self.tree.open_elements[::-1]:
            node_name = node.name
            new_phase = None
            if node == self.tree.open_elements[0]:
                assert self.container
                last = True
                node_name = self.container
            # Check for conditions that should only happen in the fragment case.
            if node_name in ("select", "colgroup", "head", "html"):
                assert self.container

            if not last and node.namespace != self.tree.default_namespace:
                continue

            if node_name in new_modes:
                new_phase = self.phases[new_modes[node_name]]
                break
            elif last:
                new_phase = self.phases["in body"]
                break

        self.phase = new_phase

    def parse_rcdata_rawtext(self, token, content_type):
        # Generic RCDATA/RAWTEXT Parsing algorithm.
        assert content_type in ("RAWTEXT", "RCDATA")

        self.tree.insert_element(token)

        if content_type == "RAWTEXT":
            self.tokenizer.state = self.tokenizer.rawtext_state
        else:
            self.tokenizer.state = self.tokenizer.rcdata_state

        self.original_phase = self.phase

        self.phase = self.phases["text"]


def dispatch(items):
    return {
        key: value
        for keys, value in items
        for key in ((keys,) if isinstance(keys, str) else keys)
    }


class Phase:
    """Base class for helper that implements each phase of processing."""
    __slots__ = ("parser", "tree", "__start_tag_cache", "__end_tag_cache")

    def __init__(self, parser, tree):
        self.parser = parser
        self.tree = tree
        self.__start_tag_cache = {}
        self.__end_tag_cache = {}

    def process_eof(self):  # pragma: no cover
        raise NotImplementedError

    def process_comment(self, token):
        # For most phases the following is correct. Where it's not it will be
        # overridden.
        self.tree.insert_comment(token, self.tree.open_elements[-1])

    def process_doctype(self, token):
        self.parser.parse_error("unexpected-doctype")

    def process_characters(self, token):
        self.tree.insert_text(token["data"])

    def process_space_characters(self, token):
        self.tree.insert_text(token["data"])

    def process_start_tag(self, token):
        name = token["name"]
        # In Py3, `in` is quicker when there are few cache hits (typically
        # short inputs).
        if name in self.__start_tag_cache:
            function = self.__start_tag_cache[name]
        else:
            function = self.__start_tag_cache[name] = self.start_tag_handler.get(
                name, type(self).start_tag_other)
            # Bound the cache size in case we get loads of unknown tags.
            while len(self.__start_tag_cache) > len(self.start_tag_handler) * 1.1:
                # This makes the eviction policy random on Py < 3.7 and FIFO >= 3.7.
                self.__start_tag_cache.pop(next(iter(self.__start_tag_cache)))
        return function(self, token)

    def start_tag_html(self, token):
        if not self.parser.first_start_tag and token["name"] == "html":
            self.parser.parse_error("non-html-root")
        # XXX Need a check here to see if the first start tag token emitted is
        # this token... If it's not, invoke self.parser.parse_error().
        for attr, value in token["data"].items():
            if attr not in self.tree.open_elements[0].attributes:
                self.tree.open_elements[0].attributes[attr] = value
        self.parser.first_start_tag = False

    def process_end_tag(self, token):
        name = token["name"]
        # In Py3, `in` is quicker when there are few cache hits (typically
        # short inputs).
        if name in self.__end_tag_cache:
            function = self.__end_tag_cache[name]
        else:
            function = self.__end_tag_cache[name] = self.end_tag_handler.get(
                name, type(self).end_tag_other)
            # Bound the cache size in case we get loads of unknown tags.
            while len(self.__end_tag_cache) > len(self.end_tag_handler) * 1.1:
                # This makes the eviction policy random on Py < 3.7 and FIFO >= 3.7.
                self.__end_tag_cache.pop(next(iter(self.__end_tag_cache)))
        return function(self, token)


class InitialPhase(Phase):
    __slots__ = tuple()

    def process_space_characters(self, token):
        pass

    def process_comment(self, token):
        self.tree.insert_comment(token, self.tree.document)

    def process_doctype(self, token):
        name = token["name"]
        public_id = token["publicId"]
        system_id = token["systemId"]
        correct = token["correct"]

        if (name != "html" or public_id is not None or
                system_id is not None and system_id != "about:legacy-compat"):
            self.parser.parse_error("unknown-doctype")

        if public_id is None:
            public_id = ""

        self.tree.insert_doctype(token)

        if public_id != "":
            public_id = public_id.translate(ascii_upper_to_lower)

        if (not correct or token["name"] != "html" or
                public_id.startswith(
                    ("+//silmaril//dtd html pro v0r11 19970101//",
                     "-//advasoft ltd//dtd html 3.0 aswedit + extensions//",
                     "-//as//dtd html 3.0 aswedit + extensions//",
                     "-//ietf//dtd html 2.0 level 1//",
                     "-//ietf//dtd html 2.0 level 2//",
                     "-//ietf//dtd html 2.0 strict level 1//",
                     "-//ietf//dtd html 2.0 strict level 2//",
                     "-//ietf//dtd html 2.0 strict//",
                     "-//ietf//dtd html 2.0//",
                     "-//ietf//dtd html 2.1e//",
                     "-//ietf//dtd html 3.0//",
                     "-//ietf//dtd html 3.2 final//",
                     "-//ietf//dtd html 3.2//",
                     "-//ietf//dtd html 3//",
                     "-//ietf//dtd html level 0//",
                     "-//ietf//dtd html level 1//",
                     "-//ietf//dtd html level 2//",
                     "-//ietf//dtd html level 3//",
                     "-//ietf//dtd html strict level 0//",
                     "-//ietf//dtd html strict level 1//",
                     "-//ietf//dtd html strict level 2//",
                     "-//ietf//dtd html strict level 3//",
                     "-//ietf//dtd html strict//",
                     "-//ietf//dtd html//",
                     "-//metrius//dtd metrius presentational//",
                     "-//microsoft//dtd internet explorer 2.0 html strict//",
                     "-//microsoft//dtd internet explorer 2.0 html//",
                     "-//microsoft//dtd internet explorer 2.0 tables//",
                     "-//microsoft//dtd internet explorer 3.0 html strict//",
                     "-//microsoft//dtd internet explorer 3.0 html//",
                     "-//microsoft//dtd internet explorer 3.0 tables//",
                     "-//netscape comm. corp.//dtd html//",
                     "-//netscape comm. corp.//dtd strict html//",
                     "-//o'reilly and associates//dtd html 2.0//",
                     "-//o'reilly and associates//dtd html extended 1.0//",
                     "-//o'reilly and associates//dtd html extended relaxed 1.0//",
                     "-//softquad software//dtd hotmetal pro 6.0::19990601::"
                     "extensions to html 4.0//",
                     "-//softquad//dtd hotmetal pro 4.0::19971010::"
                     "extensions to html 4.0//",
                     "-//spyglass//dtd html 2.0 extended//",
                     "-//sq//dtd html 2.0 hotmetal + extensions//",
                     "-//sun microsystems corp.//dtd hotjava html//",
                     "-//sun microsystems corp.//dtd hotjava strict html//",
                     "-//w3c//dtd html 3 1995-03-24//",
                     "-//w3c//dtd html 3.2 draft//",
                     "-//w3c//dtd html 3.2 final//",
                     "-//w3c//dtd html 3.2//",
                     "-//w3c//dtd html 3.2s draft//",
                     "-//w3c//dtd html 4.0 frameset//",
                     "-//w3c//dtd html 4.0 transitional//",
                     "-//w3c//dtd html experimental 19960712//",
                     "-//w3c//dtd html experimental 970421//",
                     "-//w3c//dtd w3 html//",
                     "-//w3o//dtd w3 html 3.0//",
                     "-//webtechs//dtd mozilla html 2.0//",
                     "-//webtechs//dtd mozilla html//")) or
                public_id in ("-//w3o//dtd w3 html strict 3.0//en//",
                              "-/w3c/dtd html 4.0 transitional/en",
                              "html") or
                public_id.startswith(
                    ("-//w3c//dtd html 4.01 frameset//",
                     "-//w3c//dtd html 4.01 transitional//")) and
                system_id is None or
                system_id and system_id.lower() ==
                "http://www.ibm.com/data/dtd/v11/ibmxhtml1-transitional.dtd"):
            self.parser.compatibility_mode = "quirks"
        elif (public_id.startswith(
                ("-//w3c//dtd xhtml 1.0 frameset//",
                 "-//w3c//dtd xhtml 1.0 transitional//")) or
              public_id.startswith(
                  ("-//w3c//dtd html 4.01 frameset//",
                   "-//w3c//dtd html 4.01 transitional//")) and
              system_id is not None):
            self.parser.compatibility_mode = "limited quirks"

        self.parser.phase = self.parser.phases["before html"]

    def anything_else(self):
        self.parser.compatibility_mode = "quirks"
        self.parser.phase = self.parser.phases["before html"]

    def process_characters(self, token):
        self.parser.parse_error("expected-doctype-but-got-chars")
        self.anything_else()
        return token

    def process_start_tag(self, token):
        self.parser.parse_error(
            "expected-doctype-but-got-start-tag", {"name": token["name"]})
        self.anything_else()
        return token

    def process_end_tag(self, token):
        self.parser.parse_error(
            "expected-doctype-but-got-end-tag", {"name": token["name"]})
        self.anything_else()
        return token

    def process_eof(self):
        self.parser.parse_error("expected-doctype-but-got-eof")
        self.anything_else()
        return True


class BeforeHtmlPhase(Phase):
    __slots__ = tuple()

    def _insert_html_element(self):
        self.tree.insert_root(implied_tag_token("html", "START_TAG"))
        self.parser.phase = self.parser.phases["before head"]

    def process_eof(self):
        self._insert_html_element()
        return True

    def process_comment(self, token):
        self.tree.insert_comment(token, self.tree.document)

    def process_space_characters(self, token):
        pass

    def process_characters(self, token):
        self._insert_html_element()
        return token

    def process_start_tag(self, token):
        if token["name"] == "html":
            self.parser.first_start_tag = True
        self._insert_html_element()
        return token

    def process_end_tag(self, token):
        if token["name"] not in ("head", "body", "html", "br"):
            self.parser.parse_error(
                "unexpected-end-tag-before-html", {"name": token["name"]})
        else:
            self._insert_html_element()
            return token


class BeforeHeadPhase(Phase):
    __slots__ = tuple()

    def process_eof(self):
        self.start_tag_head(implied_tag_token("head", "START_TAG"))
        return True

    def process_space_characters(self, token):
        pass

    def process_characters(self, token):
        self.start_tag_head(implied_tag_token("head", "START_TAG"))
        return token

    def start_tag_html(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_head(self, token):
        self.tree.insert_element(token)
        self.tree.head_element = self.tree.open_elements[-1]
        self.parser.phase = self.parser.phases["in head"]

    def start_tag_other(self, token):
        self.start_tag_head(implied_tag_token("head", "START_TAG"))
        return token

    def end_tag_imply_head(self, token):
        self.start_tag_head(implied_tag_token("head", "START_TAG"))
        return token

    def end_tag_other(self, token):
        self.parser.parse_error("end-tag-after-implied-root", {"name": token["name"]})

    start_tag_handler = dispatch([
        ("html", start_tag_html),
        ("head", start_tag_head)
    ])

    end_tag_handler = dispatch([
        (("head", "body", "html", "br"), end_tag_imply_head)
    ])


class InHeadPhase(Phase):
    __slots__ = tuple()

    # the real thing
    def process_eof(self):
        self.anything_else()
        return True

    def process_characters(self, token):
        self.anything_else()
        return token

    def start_tag_html(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_head(self, token):
        self.parser.parse_error("two-heads-are-not-better-than-one")

    def start_tag_base_link_command(self, token):
        self.tree.insert_element(token)
        self.tree.open_elements.pop()
        token["selfClosingAcknowledged"] = True

    def start_tag_meta(self, token):
        self.tree.insert_element(token)
        self.tree.open_elements.pop()
        token["selfClosingAcknowledged"] = True

        attributes = token["data"]
        if self.parser.tokenizer.stream.encoding[1] == "tentative":
            if "charset" in attributes:
                self.parser.tokenizer.stream.change_encoding(attributes["charset"])
            elif ("content" in attributes and
                  "http-equiv" in attributes and
                  attributes["http-equiv"].lower() == "content-type"):
                # Encoding it as UTF-8 here is a hack, as really we should pass
                # the abstract Unicode string, and just use the
                # ContentAttributeParser on that, but using UTF-8 allows all chars
                # to be encoded and as a ASCII-superset works.
                data = inputstream.EncodingBytes(attributes["content"].encode("utf-8"))
                parser = inputstream.ContentAttributeParser(data)
                codec = parser.parse()
                self.parser.tokenizer.stream.change_encoding(codec)

    def start_tag_title(self, token):
        self.parser.parse_rcdata_rawtext(token, "RCDATA")

    def start_tag_noframes_style(self, token):
        # Need to decide whether to implement the scripting-disabled case
        self.parser.parse_rcdata_rawtext(token, "RAWTEXT")

    def start_tag_noscript(self, token):
        if self.parser.scripting:
            self.parser.parse_rcdata_rawtext(token, "RAWTEXT")
        else:
            self.tree.insert_element(token)
            self.parser.phase = self.parser.phases["in head noscript"]

    def start_tag_script(self, token):
        self.tree.insert_element(token)
        self.parser.tokenizer.state = self.parser.tokenizer.script_data_state
        self.parser.original_phase = self.parser.phase
        self.parser.phase = self.parser.phases["text"]

    def start_tag_other(self, token):
        self.anything_else()
        return token

    def end_tag_head(self, token):
        node = self.parser.tree.open_elements.pop()
        assert node.name == "head", "Expected head got %s" % node.name
        self.parser.phase = self.parser.phases["after head"]

    def end_tag_html_body_br(self, token):
        self.anything_else()
        return token

    def end_tag_other(self, token):
        self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def anything_else(self):
        self.end_tag_head(implied_tag_token("head"))

    start_tag_handler = dispatch([
        ("html", start_tag_html),
        ("title", start_tag_title),
        (("noframes", "style"), start_tag_noframes_style),
        ("noscript", start_tag_noscript),
        ("script", start_tag_script),
        (("base", "basefont", "bgsound", "command", "link"),
         start_tag_base_link_command),
        ("meta", start_tag_meta),
        ("head", start_tag_head)
    ])

    end_tag_handler = dispatch([
        ("head", end_tag_head),
        (("br", "html", "body"), end_tag_html_body_br)
    ])


class InHeadNoscriptPhase(Phase):
    __slots__ = tuple()

    def process_eof(self):
        self.parser.parse_error("eof-in-head-noscript")
        self.anything_else()
        return True

    def process_comment(self, token):
        return self.parser.phases["in head"].process_comment(token)

    def process_characters(self, token):
        self.parser.parse_error("char-in-head-noscript")
        self.anything_else()
        return token

    def process_space_characters(self, token):
        return self.parser.phases["in head"].process_space_characters(token)

    def start_tag_html(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_base_link_command(self, token):
        return self.parser.phases["in head"].process_start_tag(token)

    def start_tag_head_noscript(self, token):
        self.parser.parse_error("unexpected-start-tag", {"name": token["name"]})

    def start_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-inhead-noscript-tag", {"name": token["name"]})
        self.anything_else()
        return token

    def end_tag_noscript(self, token):
        node = self.parser.tree.open_elements.pop()
        assert node.name == "noscript", f"Expected noscript got {node.name}"
        self.parser.phase = self.parser.phases["in head"]

    def end_tag_br(self, token):
        self.parser.parse_error(
            "unexpected-inhead-noscript-tag", {"name": token["name"]})
        self.anything_else()
        return token

    def end_tag_other(self, token):
        self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def anything_else(self):
        # Caller must raise parse error first!
        self.end_tag_noscript(implied_tag_token("noscript"))

    start_tag_handler = dispatch([
        ("html", start_tag_html),
        (("basefont", "bgsound", "link", "meta", "noframes", "style"),
         start_tag_base_link_command),
        (("head", "noscript"), start_tag_head_noscript),
    ])

    end_tag_handler = dispatch([
        ("noscript", end_tag_noscript),
        ("br", end_tag_br),
    ])


class AfterHeadPhase(Phase):
    __slots__ = tuple()

    def process_eof(self):
        self.anything_else()
        return True

    def process_characters(self, token):
        self.anything_else()
        return token

    def start_tag_html(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_body(self, token):
        self.parser.frameset_ok = False
        self.tree.insert_element(token)
        self.parser.phase = self.parser.phases["in body"]

    def start_tag_frameset(self, token):
        self.tree.insert_element(token)
        self.parser.phase = self.parser.phases["in frameset"]

    def start_tag_from_head(self, token):
        self.parser.parse_error(
            "unexpected-start-tag-out-of-my-head", {"name": token["name"]})
        self.tree.open_elements.append(self.tree.head_element)
        self.parser.phases["in head"].process_start_tag(token)
        for node in self.tree.open_elements[::-1]:
            if node.name == "head":
                self.tree.open_elements.remove(node)
                break

    def start_tag_head(self, token):
        self.parser.parse_error("unexpected-start-tag", {"name": token["name"]})

    def start_tag_other(self, token):
        self.anything_else()
        return token

    def end_tag_html_body_br(self, token):
        self.anything_else()
        return token

    def end_tag_other(self, token):
        self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def anything_else(self):
        self.tree.insert_element(implied_tag_token("body", "START_TAG"))
        self.parser.phase = self.parser.phases["in body"]
        self.parser.frameset_ok = True

    start_tag_handler = dispatch([
        ("html", start_tag_html),
        ("body", start_tag_body),
        ("frameset", start_tag_frameset),
        (("base", "basefont", "bgsound", "link", "meta", "noframes", "script",
          "style", "title"), start_tag_from_head),
        ("head", start_tag_head)
    ])
    end_tag_handler = dispatch([
        (("body", "html", "br"), end_tag_html_body_br)
    ])


class InBodyPhase(Phase):
    # https://www.whatwg.org/specs/web-apps/current-work/#parsing-main-inbody
    # The really-really-really-very crazy mode.
    __slots__ = ("process_space_characters",)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set this to the default handler.
        self.process_space_characters = self.process_space_characters_non_pre

    def is_matching_formatting_element(self, node1, node2):
        return (
            node1.name == node2.name and
            node1.namespace == node2.namespace and
            node1.attributes == node2.attributes)

    def add_formatting_element(self, token):
        self.tree.insert_element(token)
        element = self.tree.open_elements[-1]

        matching_elements = []
        for node in self.tree.active_formatting_elements[::-1]:
            if node is Marker:
                break
            elif self.is_matching_formatting_element(node, element):
                matching_elements.append(node)

        assert len(matching_elements) <= 3
        if len(matching_elements) == 3:
            self.tree.active_formatting_elements.remove(matching_elements[-1])
        self.tree.active_formatting_elements.append(element)

    # The real deal.
    def process_eof(self):
        allowed_elements = frozenset((
            "dd", "dt", "li", "p",
            "tbody", "td", "tfoot", "th", "thead", "tr",
            "body", "html"))
        for node in self.tree.open_elements[::-1]:
            if node.name not in allowed_elements:
                self.parser.parse_error("expected-closing-tag-but-got-eof")
                break
        # Stop parsing.

    def process_space_characters_drop_newline(self, token):
        # Sometimes (start of <pre>, <listing>, and <textarea> blocks) we
        # want to drop leading newlines.
        data = token["data"]
        self.process_space_characters = self.process_space_characters_non_pre
        if (data.startswith("\n") and
            self.tree.open_elements[-1].name in ("pre", "listing", "textarea") and
                not self.tree.open_elements[-1].has_content()):
            data = data[1:]
        if data:
            self.tree.reconstruct_active_formatting_elements()
            self.tree.insert_text(data)

    def process_characters(self, token):
        if token["data"] == "\u0000":
            # The tokenizer should always emit null on its own.
            return
        self.tree.reconstruct_active_formatting_elements()
        self.tree.insert_text(token["data"])
        # This must be bad for performance
        if self.parser.frameset_ok and any(
                char not in space_characters for char in token["data"]):
            self.parser.frameset_ok = False

    def process_space_characters_non_pre(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.tree.insert_text(token["data"])

    def start_tag_process_in_head(self, token):
        return self.parser.phases["in head"].process_start_tag(token)

    def start_tag_body(self, token):
        self.parser.parse_error("unexpected-start-tag", {"name": "body"})
        if (len(self.tree.open_elements) == 1 or
                self.tree.open_elements[1].name != "body"):
            assert self.parser.container
        else:
            self.parser.frameset_ok = False
            for attr, value in token["data"].items():
                if attr not in self.tree.open_elements[1].attributes:
                    self.tree.open_elements[1].attributes[attr] = value

    def start_tag_frameset(self, token):
        self.parser.parse_error("unexpected-start-tag", {"name": "frameset"})
        if (len(self.tree.open_elements) == 1 or
                self.tree.open_elements[1].name != "body"):
            assert self.parser.container
        elif not self.parser.frameset_ok:
            pass
        else:
            if self.tree.open_elements[1].parent:
                self.tree.open_elements[1].parent.remove_child(
                    self.tree.open_elements[1])
            while self.tree.open_elements[-1].name != "html":
                self.tree.open_elements.pop()
            self.tree.insert_element(token)
            self.parser.phase = self.parser.phases["in frameset"]

    def start_tag_close_p(self, token):
        if self.tree.element_in_scope("p", variant="button"):
            self.end_tag_p(implied_tag_token("p"))
        self.tree.insert_element(token)

    def start_tag_pre_listing(self, token):
        if self.tree.element_in_scope("p", variant="button"):
            self.end_tag_p(implied_tag_token("p"))
        self.tree.insert_element(token)
        self.parser.frameset_ok = False
        self.process_space_characters = self.process_space_characters_drop_newline

    def start_tag_form(self, token):
        if self.tree.form_element:
            self.parser.parse_error("unexpected-start-tag", {"name": "form"})
        else:
            if self.tree.element_in_scope("p", variant="button"):
                self.end_tag_p(implied_tag_token("p"))
            self.tree.insert_element(token)
            self.tree.form_element = self.tree.open_elements[-1]

    def start_tag_list_item(self, token):
        self.parser.frameset_ok = False

        stop_names_map = {"li": ["li"], "dt": ["dt", "dd"], "dd": ["dt", "dd"]}
        stop_names = stop_names_map[token["name"]]
        for node in reversed(self.tree.open_elements):
            if node.name in stop_names:
                self.parser.phase.process_end_tag(
                    implied_tag_token(node.name))
                break
            if (node.name_tuple in special_elements and
                    node.name not in ("address", "div", "p")):
                break

        if self.tree.element_in_scope("p", variant="button"):
            self.parser.phase.process_end_tag(implied_tag_token("p"))

        self.tree.insert_element(token)

    def start_tag_plaintext(self, token):
        if self.tree.element_in_scope("p", variant="button"):
            self.end_tag_p(implied_tag_token("p"))
        self.tree.insert_element(token)
        self.parser.tokenizer.state = self.parser.tokenizer.plaintext_state

    def start_tag_heading(self, token):
        if self.tree.element_in_scope("p", variant="button"):
            self.end_tag_p(implied_tag_token("p"))
        if self.tree.open_elements[-1].name in heading_elements:
            self.parser.parse_error("unexpected-start-tag", {"name": token["name"]})
            self.tree.open_elements.pop()
        self.tree.insert_element(token)

    def start_tag_a(self, token):
        afe_a_element = self.tree.element_in_active_formatting_elements("a")
        if afe_a_element:
            self.parser.parse_error(
                "unexpected-start-tag-implies-end-tag",
                {"startName": "a", "endName": "a"})
            self.end_tag_formatting(implied_tag_token("a"))
            if afe_a_element in self.tree.open_elements:
                self.tree.open_elements.remove(afe_a_element)
            if afe_a_element in self.tree.active_formatting_elements:
                self.tree.active_formatting_elements.remove(afe_a_element)
        self.tree.reconstruct_active_formatting_elements()
        self.add_formatting_element(token)

    def start_tag_formatting(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.add_formatting_element(token)

    def start_tag_nobr(self, token):
        self.tree.reconstruct_active_formatting_elements()
        if self.tree.element_in_scope("nobr"):
            self.parser.parse_error(
                "unexpected-start-tag-implies-end-tag",
                {"startName": "nobr", "endName": "nobr"})
            self.process_end_tag(implied_tag_token("nobr"))
            # XXX Need tests that trigger the following
            self.tree.reconstruct_active_formatting_elements()
        self.add_formatting_element(token)

    def start_tag_button(self, token):
        if self.tree.element_in_scope("button"):
            self.parser.parse_error(
                "unexpected-start-tag-implies-end-tag",
                {"startName": "button", "endName": "button"})
            self.process_end_tag(implied_tag_token("button"))
            return token
        else:
            self.tree.reconstruct_active_formatting_elements()
            self.tree.insert_element(token)
            self.parser.frameset_ok = False

    def start_tag_applet_marquee_object(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.tree.insert_element(token)
        self.tree.active_formatting_elements.append(Marker)
        self.parser.frameset_ok = False

    def start_tag_xmp(self, token):
        if self.tree.element_in_scope("p", variant="button"):
            self.end_tag_p(implied_tag_token("p"))
        self.tree.reconstruct_active_formatting_elements()
        self.parser.frameset_ok = False
        self.parser.parse_rcdata_rawtext(token, "RAWTEXT")

    def start_tag_table(self, token):
        if self.parser.compatibility_mode != "quirks":
            if self.tree.element_in_scope("p", variant="button"):
                self.process_end_tag(implied_tag_token("p"))
        self.tree.insert_element(token)
        self.parser.frameset_ok = False
        self.parser.phase = self.parser.phases["in table"]

    def start_tag_void_formatting(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.tree.insert_element(token)
        self.tree.open_elements.pop()
        token["selfClosingAcknowledged"] = True
        self.parser.frameset_ok = False

    def start_tag_input(self, token):
        frameset_ok = self.parser.frameset_ok
        self.start_tag_void_formatting(token)
        if ("type" in token["data"] and
                token["data"]["type"].translate(ascii_upper_to_lower) == "hidden"):
            # input type=hidden doesn't change frameset_ok
            self.parser.frameset_ok = frameset_ok

    def start_tag_param_source(self, token):
        self.tree.insert_element(token)
        self.tree.open_elements.pop()
        token["selfClosingAcknowledged"] = True

    def start_tag_hr(self, token):
        if self.tree.element_in_scope("p", variant="button"):
            self.end_tag_p(implied_tag_token("p"))
        self.tree.insert_element(token)
        self.tree.open_elements.pop()
        token["selfClosingAcknowledged"] = True
        self.parser.frameset_ok = False

    def start_tag_image(self, token):
        # No really...
        self.parser.parse_error(
            "unexpected-start-tag-treated-as",
            {"originalName": "image", "newName": "img"})
        self.process_start_tag(implied_tag_token(
            "img", "START_TAG", attributes=token["data"],
            self_closing=token["selfClosing"]))

    def start_tag_isindex(self, token):
        self.parser.parse_error("deprecated-tag", {"name": "isindex"})
        if self.tree.form_element:
            return
        form_attrs = {}
        if "action" in token["data"]:
            form_attrs["action"] = token["data"]["action"]
        self.process_start_tag(
            implied_tag_token("form", "START_TAG", attributes=form_attrs))
        self.process_start_tag(implied_tag_token("hr", "START_TAG"))
        self.process_start_tag(implied_tag_token("label", "START_TAG"))
        # XXX Localization ...
        if "prompt" in token["data"]:
            prompt = token["data"]["prompt"]
        else:
            prompt = "This is a searchable index. Enter search keywords: "
        self.process_characters({"type": Token.CHARACTERS, "data": prompt})
        attributes = token["data"].copy()
        if "action" in attributes:
            del attributes["action"]
        if "prompt" in attributes:
            del attributes["prompt"]
        attributes["name"] = "isindex"
        self.process_start_tag(implied_tag_token(
            "input", "START_TAG", attributes=attributes,
            self_closing=token["selfClosing"]))
        self.process_end_tag(implied_tag_token("label"))
        self.process_start_tag(implied_tag_token("hr", "START_TAG"))
        self.process_end_tag(implied_tag_token("form"))

    def start_tag_textarea(self, token):
        self.tree.insert_element(token)
        self.parser.tokenizer.state = self.parser.tokenizer.rcdata_state
        self.process_space_characters = self.process_space_characters_drop_newline
        self.parser.frameset_ok = False

    def start_tag_iframe(self, token):
        self.parser.frameset_ok = False
        self.start_tag_rawtext(token)

    def start_tag_noscript(self, token):
        if self.parser.scripting:
            self.start_tag_rawtext(token)
        else:
            self.start_tag_other(token)

    def start_tag_rawtext(self, token):
        self.parser.parse_rcdata_rawtext(token, "RAWTEXT")

    def start_tag_opt(self, token):
        if self.tree.open_elements[-1].name == "option":
            self.parser.phase.process_end_tag(implied_tag_token("option"))
        self.tree.reconstruct_active_formatting_elements()
        self.parser.tree.insert_element(token)

    def start_tag_select(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.tree.insert_element(token)
        self.parser.frameset_ok = False
        if self.parser.phase in (
                self.parser.phases["in table"],
                self.parser.phases["in caption"],
                self.parser.phases["in column group"],
                self.parser.phases["in table body"],
                self.parser.phases["in row"],
                self.parser.phases["in cell"]):
            self.parser.phase = self.parser.phases["in select in table"]
        else:
            self.parser.phase = self.parser.phases["in select"]

    def start_tag_rp_rt(self, token):
        if self.tree.element_in_scope("ruby"):
            self.tree.generate_implied_end_tags()
            if self.tree.open_elements[-1].name != "ruby":
                self.parser.parse_error("rp-or-rt-tag-not-in-ruby-scope")
        self.tree.insert_element(token)

    def start_tag_math(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.parser.adjust_mathml_attributes(token)
        self.parser.adjust_foreign_attributes(token)
        token["namespace"] = namespaces["mathml"]
        self.tree.insert_element(token)
        # Need to get the parse error right for the case where the token has a
        # namespace not equal to the xmlns attribute.
        if token["selfClosing"]:
            self.tree.open_elements.pop()
            token["selfClosingAcknowledged"] = True

    def start_tag_svg(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.parser.adjust_svg_attributes(token)
        self.parser.adjust_foreign_attributes(token)
        token["namespace"] = namespaces["svg"]
        self.tree.insert_element(token)
        # Need to get the parse error right for the case where the token has a
        # namespace not equal to the xmlns attribute.
        if token["selfClosing"]:
            self.tree.open_elements.pop()
            token["selfClosingAcknowledged"] = True

    def start_tag_misplaced(self, token):
        """Elements that should be children of other elements.

        Here they are ignored: "caption", "col", "colgroup", "frame",
        "frameset", "head", "option", "optgroup", "tbody", "td", "tfoot",
        "th", "thead", "tr", "noscript".

        """
        self.parser.parse_error("unexpected-start-tag-ignored", {"name": token["name"]})

    def start_tag_other(self, token):
        self.tree.reconstruct_active_formatting_elements()
        self.tree.insert_element(token)

    def end_tag_p(self, token):
        if not self.tree.element_in_scope("p", variant="button"):
            self.start_tag_close_p(implied_tag_token("p", "START_TAG"))
            self.parser.parse_error("unexpected-end-tag", {"name": "p"})
            self.end_tag_p(implied_tag_token("p"))
        else:
            self.tree.generate_implied_end_tags("p")
            if self.tree.open_elements[-1].name != "p":
                self.parser.parse_error("unexpected-end-tag", {"name": "p"})
            node = self.tree.open_elements.pop()
            while node.name != "p":
                node = self.tree.open_elements.pop()

    def end_tag_body(self, token):
        if not self.tree.element_in_scope("body"):
            self.parser.parse_error("unexpected-end-tag", {"name": "body"})
            return
        elif self.tree.open_elements[-1].name != "body":
            for node in self.tree.open_elements[2:]:
                if node.name not in frozenset((
                        "dd", "dt", "li", "optgroup", "option", "p", "rp", "rt",
                        "tbody", "td", "tfoot", "th", "thead", "tr", "body", "html")):
                    # Not sure this is the correct name for the parse error.
                    self.parser.parse_error(
                        "expected-one-end-tag-but-got-another",
                        {"gotName": "body", "expectedName": node.name})
                    break
        self.parser.phase = self.parser.phases["after body"]

    def end_tag_html(self, token):
        # We repeat the test for the body end tag token being ignored here.
        if self.tree.element_in_scope("body"):
            self.end_tag_body(implied_tag_token("body"))
            return token

    def end_tag_block(self, token):
        # Put us back in the right whitespace handling mode.
        if token["name"] == "pre":
            self.process_space_characters = self.process_space_characters_non_pre
        in_scope = self.tree.element_in_scope(token["name"])
        if in_scope:
            self.tree.generate_implied_end_tags()
        if self.tree.open_elements[-1].name != token["name"]:
            self.parser.parse_error("end-tag-too-early", {"name": token["name"]})
        if in_scope:
            node = self.tree.open_elements.pop()
            while node.name != token["name"]:
                node = self.tree.open_elements.pop()

    def end_tag_form(self, token):
        node = self.tree.form_element
        self.tree.form_element = None
        if node is None or not self.tree.element_in_scope(node):
            self.parser.parse_error("unexpected-end-tag", {"name": "form"})
        else:
            self.tree.generate_implied_end_tags()
            if self.tree.open_elements[-1] != node:
                self.parser.parse_error("end-tag-too-early-ignored", {"name": "form"})
            self.tree.open_elements.remove(node)

    def end_tag_list_item(self, token):
        if token["name"] == "li":
            variant = "list"
        else:
            variant = None
        if not self.tree.element_in_scope(token["name"], variant=variant):
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})
        else:
            self.tree.generate_implied_end_tags(exclude=token["name"])
            if self.tree.open_elements[-1].name != token["name"]:
                self.parser.parse_error("end-tag-too-early", {"name": token["name"]})
            node = self.tree.open_elements.pop()
            while node.name != token["name"]:
                node = self.tree.open_elements.pop()

    def end_tag_heading(self, token):
        for item in heading_elements:
            if self.tree.element_in_scope(item):
                self.tree.generate_implied_end_tags()
                break
        if self.tree.open_elements[-1].name != token["name"]:
            self.parser.parse_error("end-tag-too-early", {"name": token["name"]})

        for item in heading_elements:
            if self.tree.element_in_scope(item):
                item = self.tree.open_elements.pop()
                while item.name not in heading_elements:
                    item = self.tree.open_elements.pop()
                break

    def end_tag_formatting(self, token):
        """The much-feared adoption agency algorithm."""
        # http://svn.whatwg.org/webapps/complete.html#adoptionAgency revision 7867
        # XXX Better parseError messages appreciated.

        # Step 1.
        outer_loop_counter = 0

        # Step 2.
        while outer_loop_counter < 8:

            # Step 3.
            outer_loop_counter += 1

            # Step 4.

            # Let the formatting element be the last element in
            # the list of active formatting elements that:
            # - is between the end of the list and the last scope
            # marker in the list, if any, or the start of the list
            # otherwise, and
            # - has the same tag name as the token.
            formatting_element = self.tree.element_in_active_formatting_elements(
                token["name"])
            if (not formatting_element or (
                    formatting_element in self.tree.open_elements and
                    not self.tree.element_in_scope(formatting_element.name))):
                # If there is no such node, then abort these steps
                # and instead act as described in the "any other
                # end tag" entry below.
                self.end_tag_other(token)
                return

            # Otherwise, if there is such a node, but that node is
            # not in the stack of open elements, then this is a
            # parse error; remove the element from the list, and
            # abort these steps.
            elif formatting_element not in self.tree.open_elements:
                self.parser.parse_error("adoption-agency-1.2", {"name": token["name"]})
                self.tree.active_formatting_elements.remove(formatting_element)
                return

            # Otherwise, if there is such a node, and that node is
            # also in the stack of open elements, but the element
            # is not in scope, then this is a parse error; ignore
            # the token, and abort these steps.
            elif not self.tree.element_in_scope(formatting_element.name):
                self.parser.parse_error("adoption-agency-4.4", {"name": token["name"]})
                return

            # Otherwise, there is a formatting element and that
            # element is in the stack and is in scope. If the
            # element is not the current node, this is a parse
            # error. In any case, proceed with the algorithm as
            # written in the following steps.
            else:
                if formatting_element != self.tree.open_elements[-1]:
                    self.parser.parse_error(
                        "adoption-agency-1.3", {"name": token["name"]})

            # Step 5.

            # Let the furthest block be the topmost node in the
            # stack of open elements that is lower in the stack
            # than the formatting element, and is an element in
            # the special category. There might not be one.
            afe_index = self.tree.open_elements.index(formatting_element)
            furthest_block = None
            for element in self.tree.open_elements[afe_index:]:
                if element.name_tuple in special_elements:
                    furthest_block = element
                    break

            # Step 6.

            # If there is no furthest block, then the UA must
            # first pop all the nodes from the bottom of the stack
            # of open elements, from the current node up to and
            # including the formatting element, then remove the
            # formatting element from the list of active
            # formatting elements, and finally abort these steps.
            if furthest_block is None:
                element = self.tree.open_elements.pop()
                while element != formatting_element:
                    element = self.tree.open_elements.pop()
                self.tree.active_formatting_elements.remove(element)
                return

            # Step 7.
            common_ancestor = self.tree.open_elements[afe_index - 1]

            # Step 8.

            # The bookmark is supposed to help us identify where to reinsert
            # nodes in step 15. We have to ensure that we reinsert nodes after
            # the node before the active formatting element. Note the bookmark
            # can move in step 9.7.
            bookmark = self.tree.active_formatting_elements.index(formatting_element)

            # Step 9.
            last_node = node = furthest_block
            inner_loop_counter = 0

            index = self.tree.open_elements.index(node)
            while inner_loop_counter < 3:
                inner_loop_counter += 1
                # Node is element before node in open elements.
                index -= 1
                node = self.tree.open_elements[index]
                if node not in self.tree.active_formatting_elements:
                    self.tree.open_elements.remove(node)
                    continue
                # Step 9.6.
                if node == formatting_element:
                    break
                # Step 9.7.
                if last_node == furthest_block:
                    bookmark = self.tree.active_formatting_elements.index(node) + 1
                # Step 9.8.
                clone = node.clone()
                # Replace node with clone
                self.tree.active_formatting_elements[
                    self.tree.active_formatting_elements.index(node)] = clone
                self.tree.open_elements[self.tree.open_elements.index(node)] = clone
                node = clone
                # Step 9.9.
                # Remove lastNode from its parents, if any
                if last_node.parent:
                    last_node.parent.remove_child(last_node)
                node.append_child(last_node)
                # Step 9.10.
                last_node = node

            # Step 10.

            # Foster parent lastNode if commonAncestor is a
            # table, tbody, tfoot, thead, or tr we need to foster
            # parent the lastNode
            if last_node.parent:
                last_node.parent.remove_child(last_node)

            if common_ancestor.name in frozenset((
                    "table", "tbody", "tfoot", "thead", "tr")):
                parent, insert_before = self.tree.get_table_misnested_node_position()
                parent.insert_before(last_node, insert_before)
            else:
                common_ancestor.append_child(last_node)

            # Step 11
            clone = formatting_element.clone()

            # Step 12
            furthest_block.reparent_children(clone)

            # Step 13
            furthest_block.append_child(clone)

            # Step 14
            self.tree.active_formatting_elements.remove(formatting_element)
            self.tree.active_formatting_elements.insert(bookmark, clone)

            # Step 15
            self.tree.open_elements.remove(formatting_element)
            self.tree.open_elements.insert(
                self.tree.open_elements.index(furthest_block) + 1, clone)

    def end_tag_applet_marquee_object(self, token):
        if self.tree.element_in_scope(token["name"]):
            self.tree.generate_implied_end_tags()
        if self.tree.open_elements[-1].name != token["name"]:
            self.parser.parse_error("end-tag-too-early", {"name": token["name"]})

        if self.tree.element_in_scope(token["name"]):
            element = self.tree.open_elements.pop()
            while element.name != token["name"]:
                element = self.tree.open_elements.pop()
            self.tree.clear_active_formatting_elements()

    def end_tag_br(self, token):
        self.parser.parse_error(
            "unexpected-end-tag-treated-as",
            {"originalName": "br", "newName": "br element"})
        self.tree.reconstruct_active_formatting_elements()
        self.tree.insert_element(implied_tag_token("br", "START_TAG"))
        self.tree.open_elements.pop()

    def end_tag_other(self, token):
        for node in self.tree.open_elements[::-1]:
            if node.name == token["name"]:
                self.tree.generate_implied_end_tags(exclude=token["name"])
                if self.tree.open_elements[-1].name != token["name"]:
                    self.parser.parse_error(
                        "unexpected-end-tag", {"name": token["name"]})
                while self.tree.open_elements.pop() != node:
                    pass
                break
            else:
                if node.name_tuple in special_elements:
                    self.parser.parse_error(
                        "unexpected-end-tag", {"name": token["name"]})
                    break

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        (("base", "basefont", "bgsound", "command", "link", "meta",
          "script", "style", "title"), start_tag_process_in_head),
        ("body", start_tag_body),
        ("frameset", start_tag_frameset),
        (("address", "article", "aside", "blockquote", "center", "details",
          "dir", "div", "dl", "fieldset", "figcaption", "figure",
          "footer", "header", "hgroup", "main", "menu", "nav", "ol", "p",
          "section", "summary", "ul"), start_tag_close_p),
        (heading_elements, start_tag_heading),
        (("pre", "listing"), start_tag_pre_listing),
        ("form", start_tag_form),
        (("li", "dd", "dt"), start_tag_list_item),
        ("plaintext", start_tag_plaintext),
        ("a", start_tag_a),
        (("b", "big", "code", "em", "font", "i", "s", "small", "strike",
          "strong", "tt", "u"), start_tag_formatting),
        ("nobr", start_tag_nobr),
        ("button", start_tag_button),
        (("applet", "marquee", "object"), start_tag_applet_marquee_object),
        ("xmp", start_tag_xmp),
        ("table", start_tag_table),
        (("area", "br", "embed", "img", "keygen", "wbr"), start_tag_void_formatting),
        (("param", "source", "track"), start_tag_param_source),
        ("input", start_tag_input),
        ("hr", start_tag_hr),
        ("image", start_tag_image),
        ("isindex", start_tag_isindex),
        ("textarea", start_tag_textarea),
        ("iframe", start_tag_iframe),
        ("noscript", start_tag_noscript),
        (("noembed", "noframes"), start_tag_rawtext),
        ("select", start_tag_select),
        (("rp", "rt"), start_tag_rp_rt),
        (("option", "optgroup"), start_tag_opt),
        (("math"), start_tag_math),
        (("svg"), start_tag_svg),
        (("caption", "col", "colgroup", "frame", "head",
          "tbody", "td", "tfoot", "th", "thead", "tr"), start_tag_misplaced)
    ])

    end_tag_handler = dispatch([
        ("body", end_tag_body),
        ("html", end_tag_html),
        (("address", "article", "aside", "blockquote", "button", "center",
          "details", "dialog", "dir", "div", "dl", "fieldset", "figcaption", "figure",
          "footer", "header", "hgroup", "listing", "main", "menu", "nav", "ol", "pre",
          "section", "summary", "ul"), end_tag_block),
        ("form", end_tag_form),
        ("p", end_tag_p),
        (("dd", "dt", "li"), end_tag_list_item),
        (heading_elements, end_tag_heading),
        (("a", "b", "big", "code", "em", "font", "i", "nobr", "s", "small",
          "strike", "strong", "tt", "u"), end_tag_formatting),
        (("applet", "marquee", "object"), end_tag_applet_marquee_object),
        ("br", end_tag_br),
    ])


class TextPhase(Phase):
    __slots__ = tuple()

    def process_characters(self, token):
        self.tree.insert_text(token["data"])

    def process_eof(self):
        self.parser.parse_error(
            "expected-named-closing-tag-but-got-eof",
            {"name": self.tree.open_elements[-1].name})
        self.tree.open_elements.pop()
        self.parser.phase = self.parser.original_phase
        return True

    def start_tag_other(self, token):
        assert False, (  # pragma: no cover
            f"Tried to process start tag {token['name']} in RCDATA/RAWTEXT mode")

    def end_tag_script(self, token):
        node = self.tree.open_elements.pop()
        assert node.name == "script"
        self.parser.phase = self.parser.original_phase
        # The rest of this method is all stuff that only happens if
        # document.write works.

    def end_tag_other(self, token):
        self.tree.open_elements.pop()
        self.parser.phase = self.parser.original_phase

    start_tag_handler = dispatch([])
    end_tag_handler = dispatch([("script", end_tag_script)])


class InTablePhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#in-table
    __slots__ = tuple()

    def _clear_stack_to_table_context(self):
        # "Clear the stack back to a table context".
        while self.tree.open_elements[-1].name not in ("table", "html"):
            # self.parser.parse_error("unexpected-implied-end-tag-in-table",
            #  {"name":  self.tree.open_elements[-1].name})
            self.tree.open_elements.pop()
        # When the current node is <html> it's a fragment case.

    def process_eof(self):
        if self.tree.open_elements[-1].name != "html":
            self.parser.parse_error("eof-in-table")
        else:
            assert self.parser.container
        # Stop parsing.

    def process_space_characters(self, token):
        original_phase = self.parser.phase
        self.parser.phase = self.parser.phases["in table text"]
        self.parser.phase.original_phase = original_phase
        self.parser.phase.process_space_characters(token)

    def process_characters(self, token):
        original_phase = self.parser.phase
        self.parser.phase = self.parser.phases["in table text"]
        self.parser.phase.original_phase = original_phase
        self.parser.phase.process_characters(token)

    def insert_text(self, token):
        # If we get here there must be at least one non-whitespace character.
        # Do the table magic!
        self.tree.insert_from_table = True
        self.parser.phases["in body"].process_characters(token)
        self.tree.insert_from_table = False

    def start_tag_caption(self, token):
        self._clear_stack_to_table_context()
        self.tree.active_formatting_elements.append(Marker)
        self.tree.insert_element(token)
        self.parser.phase = self.parser.phases["in caption"]

    def start_tag_colgroup(self, token):
        self._clear_stack_to_table_context()
        self.tree.insert_element(token)
        self.parser.phase = self.parser.phases["in column group"]

    def start_tag_col(self, token):
        self.start_tag_colgroup(implied_tag_token("colgroup", "START_TAG"))
        return token

    def start_tag_rowgroup(self, token):
        self._clear_stack_to_table_context()
        self.tree.insert_element(token)
        self.parser.phase = self.parser.phases["in table body"]

    def start_tag_imply_tbody(self, token):
        self.start_tag_rowgroup(implied_tag_token("tbody", "START_TAG"))
        return token

    def start_tag_table(self, token):
        self.parser.parse_error(
            "unexpected-start-tag-implies-end-tag",
            {"startName": "table", "endName": "table"})
        self.parser.phase.process_end_tag(implied_tag_token("table"))
        if not self.parser.container:
            return token

    def start_tag_style_script(self, token):
        return self.parser.phases["in head"].process_start_tag(token)

    def start_tag_input(self, token):
        if ("type" in token["data"] and
                token["data"]["type"].translate(ascii_upper_to_lower) == "hidden"):
            self.parser.parse_error("unexpected-hidden-input-in-table")
            self.tree.insert_element(token)
            # XXX associate with form.
            self.tree.open_elements.pop()
        else:
            self.start_tag_other(token)

    def start_tag_form(self, token):
        self.parser.parse_error("unexpected-form-in-table")
        if self.tree.form_element is None:
            self.tree.insert_element(token)
            self.tree.form_element = self.tree.open_elements[-1]
            self.tree.open_elements.pop()

    def start_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-start-tag-implies-table-voodoo", {"name": token["name"]})
        # Do the table magic!
        self.tree.insert_from_table = True
        self.parser.phases["in body"].process_start_tag(token)
        self.tree.insert_from_table = False

    def end_tag_table(self, token):
        if self.tree.element_in_scope("table", variant="table"):
            self.tree.generate_implied_end_tags()
            if self.tree.open_elements[-1].name != "table":
                self.parser.parse_error("end-tag-too-early-named", {
                    "gotName": "table",
                    "expectedName": self.tree.open_elements[-1].name})
            while self.tree.open_elements[-1].name != "table":
                self.tree.open_elements.pop()
            self.tree.open_elements.pop()
            self.parser.reset_insertion_mode()
        else:
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_ignore(self, token):
        self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-end-tag-implies-table-voodoo", {"name": token["name"]})
        # Do the table magic!
        self.tree.insert_from_table = True
        self.parser.phases["in body"].process_end_tag(token)
        self.tree.insert_from_table = False

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        ("caption", start_tag_caption),
        ("colgroup", start_tag_colgroup),
        ("col", start_tag_col),
        (("tbody", "tfoot", "thead"), start_tag_rowgroup),
        (("td", "th", "tr"), start_tag_imply_tbody),
        ("table", start_tag_table),
        (("style", "script"), start_tag_style_script),
        ("input", start_tag_input),
        ("form", start_tag_form)
    ])

    end_tag_handler = dispatch([
        ("table", end_tag_table),
        (("body", "caption", "col", "colgroup", "html", "tbody", "td",
          "tfoot", "th", "thead", "tr"), end_tag_ignore)
    ])


class InTableTextPhase(Phase):
    __slots__ = ("original_phase", "character_tokens")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.original_phase = None
        self.character_tokens = []

    def flush_characters(self):
        data = "".join([item["data"] for item in self.character_tokens])
        if any(item not in space_characters for item in data):
            token = {"type": Token.CHARACTERS, "data": data}
            self.parser.phases["in table"].insert_text(token)
        elif data:
            self.tree.insert_text(data)
        self.character_tokens = []

    def process_comment(self, token):
        self.flush_characters()
        self.parser.phase = self.original_phase
        return token

    def process_eof(self):
        self.flush_characters()
        self.parser.phase = self.original_phase
        return True

    def process_characters(self, token):
        if token["data"] == "\u0000":
            return
        self.character_tokens.append(token)

    def process_space_characters(self, token):
        # Pretty sure we should never reach here.
        self.character_tokens.append(token)
        # assert False

    def process_start_tag(self, token):
        self.flush_characters()
        self.parser.phase = self.original_phase
        return token

    def process_end_tag(self, token):
        self.flush_characters()
        self.parser.phase = self.original_phase
        return token


class InCaptionPhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#in-caption
    __slots__ = tuple()

    def ignore_end_tag_caption(self):
        return not self.tree.element_in_scope("caption", variant="table")

    def process_eof(self):
        self.parser.phases["in body"].process_eof()

    def process_characters(self, token):
        return self.parser.phases["in body"].process_characters(token)

    def start_tag_table_element(self, token):
        self.parser.parse_error("unexpected-table-start-tag-in-caption")
        # XXX Have to duplicate logic here to find out if the tag is ignored.
        ignore_end_tag = self.ignore_end_tag_caption()
        self.parser.phase.process_end_tag(implied_tag_token("caption"))
        if not ignore_end_tag:
            return token

    def start_tag_other(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def end_tag_caption(self, token):
        if not self.ignore_end_tag_caption():
            # AT this code is quite similar to end_tag_table in "InTable".
            self.tree.generate_implied_end_tags()
            if self.tree.open_elements[-1].name != "caption":
                self.parser.parse_error("expected-one-end-tag-but-got-another", {
                    "gotName": "caption",
                    "expectedName": self.tree.open_elements[-1].name})
            while self.tree.open_elements[-1].name != "caption":
                self.tree.open_elements.pop()
            self.tree.open_elements.pop()
            self.tree.clear_active_formatting_elements()
            self.parser.phase = self.parser.phases["in table"]
        else:
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_table(self, token):
        self.parser.parse_error("unexpected-table-end-tag-in-caption")
        ignore_end_tag = self.ignore_end_tag_caption()
        self.parser.phase.process_end_tag(implied_tag_token("caption"))
        if not ignore_end_tag:
            return token

    def end_tag_ignore(self, token):
        self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_other(self, token):
        return self.parser.phases["in body"].process_end_tag(token)

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        (("caption", "col", "colgroup", "tbody", "td", "tfoot", "th",
          "thead", "tr"), start_tag_table_element)
    ])

    end_tag_handler = dispatch([
        ("caption", end_tag_caption),
        ("table", end_tag_table),
        (("body", "col", "colgroup", "html", "tbody", "td", "tfoot", "th",
          "thead", "tr"), end_tag_ignore)
    ])


class InColumnGroupPhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#in-column
    __slots__ = tuple()

    def ignore_end_tag_colgroup(self):
        return self.tree.open_elements[-1].name == "html"

    def process_eof(self):
        if self.tree.open_elements[-1].name == "html":
            assert self.parser.container
            return
        else:
            ignore_end_tag = self.ignore_end_tag_colgroup()
            self.end_tag_colgroup(implied_tag_token("colgroup"))
            if not ignore_end_tag:
                return True

    def process_characters(self, token):
        ignore_end_tag = self.ignore_end_tag_colgroup()
        self.end_tag_colgroup(implied_tag_token("colgroup"))
        if not ignore_end_tag:
            return token

    def start_tag_col(self, token):
        self.tree.insert_element(token)
        self.tree.open_elements.pop()
        token["selfClosingAcknowledged"] = True

    def start_tag_other(self, token):
        ignore_end_tag = self.ignore_end_tag_colgroup()
        self.end_tag_colgroup(implied_tag_token("colgroup"))
        if not ignore_end_tag:
            return token

    def end_tag_colgroup(self, token):
        if self.ignore_end_tag_colgroup():
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})
        else:
            self.tree.open_elements.pop()
            self.parser.phase = self.parser.phases["in table"]

    def end_tag_col(self, token):
        self.parser.parse_error("no-end-tag", {"name": "col"})

    def end_tag_other(self, token):
        ignore_end_tag = self.ignore_end_tag_colgroup()
        self.end_tag_colgroup(implied_tag_token("colgroup"))
        if not ignore_end_tag:
            return token

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        ("col", start_tag_col)
    ])

    end_tag_handler = dispatch([
        ("colgroup", end_tag_colgroup),
        ("col", end_tag_col)
    ])


class InTableBodyPhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#in-table0
    __slots__ = tuple()

    def _clear_stack_to_table_body_context(self):
        while self.tree.open_elements[-1].name not in (
                "tbody", "tfoot", "thead", "html"):
            # self.parser.parse_error("unexpected-implied-end-tag-in-table",
            #  {"name": self.tree.open_elements[-1].name})
            self.tree.open_elements.pop()
        if self.tree.open_elements[-1].name == "html":
            assert self.parser.container

    def process_eof(self):
        self.parser.phases["in table"].process_eof()

    def process_space_characters(self, token):
        return self.parser.phases["in table"].process_space_characters(token)

    def process_characters(self, token):
        return self.parser.phases["in table"].process_characters(token)

    def start_tag_tr(self, token):
        self._clear_stack_to_table_body_context()
        self.tree.insert_element(token)
        self.parser.phase = self.parser.phases["in row"]

    def start_tag_table_cell(self, token):
        self.parser.parse_error(
            "unexpected-cell-in-table-body", {"name": token["name"]})
        self.start_tag_tr(implied_tag_token("tr", "START_TAG"))
        return token

    def start_tag_table_other(self, token):
        # XXX AT Any ideas on how to share this with end_tag_table?
        if (self.tree.element_in_scope("tbody", variant="table") or
                self.tree.element_in_scope("thead", variant="table") or
                self.tree.element_in_scope("tfoot", variant="table")):
            self._clear_stack_to_table_body_context()
            self.end_tag_table_rowgroup(
                implied_tag_token(self.tree.open_elements[-1].name))
            return token
        else:
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error(
                "unexpected-start-tag-out-of-table", {"name": token["name"]})

    def start_tag_other(self, token):
        return self.parser.phases["in table"].process_start_tag(token)

    def end_tag_table_rowgroup(self, token):
        if self.tree.element_in_scope(token["name"], variant="table"):
            self._clear_stack_to_table_body_context()
            self.tree.open_elements.pop()
            self.parser.phase = self.parser.phases["in table"]
        else:
            self.parser.parse_error(
                "unexpected-end-tag-in-table-body", {"name": token["name"]})

    def end_tag_table(self, token):
        if (self.tree.element_in_scope("tbody", variant="table") or
                self.tree.element_in_scope("thead", variant="table") or
                self.tree.element_in_scope("tfoot", variant="table")):
            self._clear_stack_to_table_body_context()
            self.end_tag_table_rowgroup(
                implied_tag_token(self.tree.open_elements[-1].name))
            return token
        else:
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_ignore(self, token):
        self.parser.parse_error(
            "unexpected-end-tag-in-table-body", {"name": token["name"]})

    def end_tag_other(self, token):
        return self.parser.phases["in table"].process_end_tag(token)

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        ("tr", start_tag_tr),
        (("td", "th"), start_tag_table_cell),
        (("caption", "col", "colgroup", "tbody", "tfoot", "thead"),
        start_tag_table_other)
    ])

    end_tag_handler = dispatch([
        (("tbody", "tfoot", "thead"), end_tag_table_rowgroup),
        ("table", end_tag_table),
        (("body", "caption", "col", "colgroup", "html", "td", "th",
          "tr"), end_tag_ignore)
    ])


class InRowPhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#in-row
    __slots__ = tuple()

    def _clear_stack_to_table_row_context(self):
        while self.tree.open_elements[-1].name not in ("tr", "html"):
            self.parser.parse_error(
                "unexpected-implied-end-tag-in-table-row",
                {"name": self.tree.open_elements[-1].name})
            self.tree.open_elements.pop()

    def ignore_end_tag_tr(self):
        return not self.tree.element_in_scope("tr", variant="table")

    def process_eof(self):
        self.parser.phases["in table"].process_eof()

    def process_space_characters(self, token):
        return self.parser.phases["in table"].process_space_characters(token)

    def process_characters(self, token):
        return self.parser.phases["in table"].process_characters(token)

    def start_tag_table_cell(self, token):
        self._clear_stack_to_table_row_context()
        self.tree.insert_element(token)
        self.parser.phase = self.parser.phases["in cell"]
        self.tree.active_formatting_elements.append(Marker)

    def start_tag_table_other(self, token):
        ignore_end_tag = self.ignore_end_tag_tr()
        self.end_tag_tr(implied_tag_token("tr"))
        # XXX how are we sure it's always ignored in the fragment case?
        if not ignore_end_tag:
            return token

    def start_tag_other(self, token):
        return self.parser.phases["in table"].process_start_tag(token)

    def end_tag_tr(self, token):
        if not self.ignore_end_tag_tr():
            self._clear_stack_to_table_row_context()
            self.tree.open_elements.pop()
            self.parser.phase = self.parser.phases["in table body"]
        else:
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_table(self, token):
        ignore_end_tag = self.ignore_end_tag_tr()
        self.end_tag_tr(implied_tag_token("tr"))
        # Reprocess the current tag if the tr end tag was not ignored.
        # XXX how are we sure it's always ignored in the fragment case?
        if not ignore_end_tag:
            return token

    def end_tag_table_rowgroup(self, token):
        if self.tree.element_in_scope(token["name"], variant="table"):
            self.end_tag_tr(implied_tag_token("tr"))
            return token
        else:
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_ignore(self, token):
        self.parser.parse_error(
            "unexpected-end-tag-in-table-row", {"name": token["name"]})

    def end_tag_other(self, token):
        return self.parser.phases["in table"].process_end_tag(token)

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        (("td", "th"), start_tag_table_cell),
        (("caption", "col", "colgroup", "tbody", "tfoot", "thead",
          "tr"), start_tag_table_other)
    ])

    end_tag_handler = dispatch([
        ("tr", end_tag_tr),
        ("table", end_tag_table),
        (("tbody", "tfoot", "thead"), end_tag_table_rowgroup),
        (("body", "caption", "col", "colgroup", "html", "td", "th"), end_tag_ignore)
    ])


class InCellPhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#in-cell
    __slots__ = tuple()

    def _close_cell(self):
        if self.tree.element_in_scope("td", variant="table"):
            self.end_tag_table_cell(implied_tag_token("td"))
        elif self.tree.element_in_scope("th", variant="table"):
            self.end_tag_table_cell(implied_tag_token("th"))

    def process_eof(self):
        self.parser.phases["in body"].process_eof()

    def process_characters(self, token):
        return self.parser.phases["in body"].process_characters(token)

    def start_tag_table_other(self, token):
        if (self.tree.element_in_scope("td", variant="table") or
                self.tree.element_in_scope("th", variant="table")):
            self._close_cell()
            return token
        else:
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error(
                "unexpected-start-tag-out-of-table-cell", {"name": token["name"]})

    def start_tag_other(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def end_tag_table_cell(self, token):
        if self.tree.element_in_scope(token["name"], variant="table"):
            self.tree.generate_implied_end_tags(token["name"])
            if self.tree.open_elements[-1].name != token["name"]:
                self.parser.parse_error(
                    "unexpected-cell-end-tag", {"name": token["name"]})
                while True:
                    node = self.tree.open_elements.pop()
                    if node.name == token["name"]:
                        break
            else:
                self.tree.open_elements.pop()
            self.tree.clear_active_formatting_elements()
            self.parser.phase = self.parser.phases["in row"]
        else:
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_ignore(self, token):
        self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_imply(self, token):
        if self.tree.element_in_scope(token["name"], variant="table"):
            self._close_cell()
            return token
        else:
            # Sometimes fragment case.
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_other(self, token):
        return self.parser.phases["in body"].process_end_tag(token)

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        (("caption", "col", "colgroup", "tbody", "td", "tfoot", "th",
          "thead", "tr"), start_tag_table_other)
    ])

    end_tag_handler = dispatch([
        (("td", "th"), end_tag_table_cell),
        (("body", "caption", "col", "colgroup", "html"), end_tag_ignore),
        (("table", "tbody", "tfoot", "thead", "tr"), end_tag_imply)
    ])


class InSelectPhase(Phase):
    __slots__ = tuple()

    # http://www.whatwg.org/specs/web-apps/current-work/#in-select
    def process_eof(self):
        if self.tree.open_elements[-1].name != "html":
            self.parser.parse_error("eof-in-select")
        else:
            assert self.parser.container

    def process_characters(self, token):
        if token["data"] == "\u0000":
            return
        self.tree.insert_text(token["data"])

    def start_tag_option(self, token):
        # We need to imply </option> if <option> is the current node.
        if self.tree.open_elements[-1].name == "option":
            self.tree.open_elements.pop()
        self.tree.insert_element(token)

    def start_tag_optgroup(self, token):
        if self.tree.open_elements[-1].name == "option":
            self.tree.open_elements.pop()
        if self.tree.open_elements[-1].name == "optgroup":
            self.tree.open_elements.pop()
        self.tree.insert_element(token)

    def start_tag_select(self, token):
        self.parser.parse_error("unexpected-select-in-select")
        self.end_tag_select(implied_tag_token("select"))

    def start_tag_input(self, token):
        self.parser.parse_error("unexpected-input-in-select")
        if self.tree.element_in_scope("select", variant="select"):
            self.end_tag_select(implied_tag_token("select"))
            return token
        else:
            assert self.parser.container

    def start_tag_script(self, token):
        return self.parser.phases["in head"].process_start_tag(token)

    def start_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-start-tag-in-select", {"name": token["name"]})

    def end_tag_option(self, token):
        if self.tree.open_elements[-1].name == "option":
            self.tree.open_elements.pop()
        else:
            self.parser.parse_error("unexpected-end-tag-in-select", {"name": "option"})

    def end_tag_optgroup(self, token):
        # </optgroup> implicitly closes <option>.
        if (self.tree.open_elements[-1].name == "option" and
                self.tree.open_elements[-2].name == "optgroup"):
            self.tree.open_elements.pop()
        # It also closes </optgroup>.
        if self.tree.open_elements[-1].name == "optgroup":
            self.tree.open_elements.pop()
        # But nothing else.
        else:
            self.parser.parse_error(
                "unexpected-end-tag-in-select", {"name": "optgroup"})

    def end_tag_select(self, token):
        if self.tree.element_in_scope("select", variant="select"):
            node = self.tree.open_elements.pop()
            while node.name != "select":
                node = self.tree.open_elements.pop()
            self.parser.reset_insertion_mode()
        else:
            # Fragment case.
            assert self.parser.container
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

    def end_tag_other(self, token):
        self.parser.parse_error("unexpected-end-tag-in-select", {"name": token["name"]})

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        ("option", start_tag_option),
        ("optgroup", start_tag_optgroup),
        ("select", start_tag_select),
        (("input", "keygen", "textarea"), start_tag_input),
        ("script", start_tag_script)
    ])

    end_tag_handler = dispatch([
        ("option", end_tag_option),
        ("optgroup", end_tag_optgroup),
        ("select", end_tag_select)
    ])


class InSelectInTablePhase(Phase):
    __slots__ = tuple()

    def process_eof(self):
        self.parser.phases["in select"].process_eof()

    def process_characters(self, token):
        return self.parser.phases["in select"].process_characters(token)

    def start_tag_table(self, token):
        self.parser.parse_error(
            "unexpected-table-element-start-tag-in-select-in-table",
            {"name": token["name"]})
        self.end_tag_other(implied_tag_token("select"))
        return token

    def start_tag_other(self, token):
        return self.parser.phases["in select"].process_start_tag(token)

    def end_tag_table(self, token):
        self.parser.parse_error(
            "unexpected-table-element-end-tag-in-select-in-table",
            {"name": token["name"]})
        if self.tree.element_in_scope(token["name"], variant="table"):
            self.end_tag_other(implied_tag_token("select"))
            return token

    def end_tag_other(self, token):
        return self.parser.phases["in select"].process_end_tag(token)

    start_tag_handler = dispatch([
        (("caption", "table", "tbody", "tfoot", "thead", "tr", "td", "th"),
         start_tag_table)
    ])

    end_tag_handler = dispatch([
        (("caption", "table", "tbody", "tfoot", "thead", "tr", "td", "th"),
         end_tag_table)
    ])


class InForeignContentPhase(Phase):
    __slots__ = tuple()

    breakout_elements = frozenset([
        "b", "big", "blockquote", "body", "br", "center", "code", "dd", "div", "dl",
        "dt", "em", "embed", "h1", "h2", "h3", "h4", "h5", "h6", "head", "hr", "i",
        "img", "li", "listing", "menu", "meta", "nobr", "ol", "p", "pre", "ruby", "s",
        "small", "span", "strong", "strike", "sub", "sup", "table", "tt", "u", "ul",
        "var"])

    def adjust_svg_tag_names(self, token):
        replacements = {
            "altglyph": "altGlyph",
            "altglyphdef": "altGlyphDef",
            "altglyphitem": "altGlyphItem",
            "animatecolor": "animateColor",
            "animatemotion": "animateMotion",
            "animatetransform": "animateTransform",
            "clippath": "clipPath",
            "feblend": "feBlend",
            "fecolormatrix": "feColorMatrix",
            "fecomponenttransfer": "feComponentTransfer",
            "fecomposite": "feComposite",
            "feconvolvematrix": "feConvolveMatrix",
            "fediffuselighting": "feDiffuseLighting",
            "fedisplacementmap": "feDisplacementMap",
            "fedistantlight": "feDistantLight",
            "feflood": "feFlood",
            "fefunca": "feFuncA",
            "fefuncb": "feFuncB",
            "fefuncg": "feFuncG",
            "fefuncr": "feFuncR",
            "fegaussianblur": "feGaussianBlur",
            "feimage": "feImage",
            "femerge": "feMerge",
            "femergenode": "feMergeNode",
            "femorphology": "feMorphology",
            "feoffset": "feOffset",
            "fepointlight": "fePointLight",
            "fespecularlighting": "feSpecularLighting",
            "fespotlight": "feSpotLight",
            "fetile": "feTile",
            "feturbulence": "feTurbulence",
            "foreignobject": "foreignObject",
            "glyphref": "glyphRef",
            "lineargradient": "linearGradient",
            "radialgradient": "radialGradient",
            "textpath": "textPath",
        }

        if token["name"] in replacements:
            token["name"] = replacements[token["name"]]

    def process_characters(self, token):
        if token["data"] == "\u0000":
            token["data"] = "\uFFFD"
        elif (self.parser.frameset_ok and
              any(char not in space_characters for char in token["data"])):
            self.parser.frameset_ok = False
        Phase.process_characters(self, token)

    def process_start_tag(self, token):
        current_node = self.tree.open_elements[-1]
        if (token["name"] in self.breakout_elements or (
                token["name"] == "font" and
                set(token["data"].keys()) & {"color", "face", "size"})):
            self.parser.parse_error(
                "unexpected-html-element-in-foreign-content", {"name": token["name"]})
            while (self.tree.open_elements[-1].namespace !=
                   self.tree.default_namespace and
                   not self.parser.is_html_integration_point(
                       self.tree.open_elements[-1]) and
                   not self.parser.is_mathml_text_integration_point(
                       self.tree.open_elements[-1])):
                self.tree.open_elements.pop()
            return token

        else:
            if current_node.namespace == namespaces["mathml"]:
                self.parser.adjust_mathml_attributes(token)
            elif current_node.namespace == namespaces["svg"]:
                self.adjust_svg_tag_names(token)
                self.parser.adjust_svg_attributes(token)
            self.parser.adjust_foreign_attributes(token)
            token["namespace"] = current_node.namespace
            self.tree.insert_element(token)
            if token["selfClosing"]:
                self.tree.open_elements.pop()
                token["selfClosingAcknowledged"] = True

    def process_end_tag(self, token):
        node_index = len(self.tree.open_elements) - 1
        node = self.tree.open_elements[-1]
        if node.name.translate(ascii_upper_to_lower) != token["name"]:
            self.parser.parse_error("unexpected-end-tag", {"name": token["name"]})

        while True:
            if node.name.translate(ascii_upper_to_lower) == token["name"]:
                # XXX this isn't in the spec but it seems necessary
                if self.parser.phase == self.parser.phases["in table text"]:
                    self.parser.phase.flush_characters()
                    self.parser.phase = self.parser.phase.original_phase
                while self.tree.open_elements.pop() != node:
                    assert self.tree.open_elements
                new_token = None
                break
            node_index -= 1

            node = self.tree.open_elements[node_index]
            if node.namespace != self.tree.default_namespace:
                continue
            else:
                new_token = self.parser.phase.process_end_tag(token)
                break
        return new_token


class AfterBodyPhase(Phase):
    __slots__ = tuple()

    def process_eof(self):
        # Stop parsing
        pass

    def process_comment(self, token):
        # This is needed because data is to be appended to the <html> element
        # here and not to whatever is currently open.
        self.tree.insert_comment(token, self.tree.open_elements[0])

    def process_characters(self, token):
        self.parser.parse_error("unexpected-char-after-body")
        self.parser.phase = self.parser.phases["in body"]
        return token

    def start_tag_html(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-start-tag-after-body", {"name": token["name"]})
        self.parser.phase = self.parser.phases["in body"]
        return token

    def end_tag_html(self, name):
        if self.parser.container:
            self.parser.parse_error("unexpected-end-tag-after-body-innerhtml")
        else:
            self.parser.phase = self.parser.phases["after after body"]

    def end_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-end-tag-after-body", {"name": token["name"]})
        self.parser.phase = self.parser.phases["in body"]
        return token

    start_tag_handler = dispatch([
        ("html", start_tag_html)
    ])

    end_tag_handler = dispatch([("html", end_tag_html)])


class InFramesetPhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#in-frameset
    __slots__ = tuple()

    def process_eof(self):
        if self.tree.open_elements[-1].name != "html":
            self.parser.parse_error("eof-in-frameset")
        else:
            assert self.parser.container

    def process_characters(self, token):
        self.parser.parse_error("unexpected-char-in-frameset")

    def start_tag_frameset(self, token):
        self.tree.insert_element(token)

    def start_tag_frame(self, token):
        self.tree.insert_element(token)
        self.tree.open_elements.pop()

    def start_tag_noframes(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-start-tag-in-frameset", {"name": token["name"]})

    def end_tag_frameset(self, token):
        if self.tree.open_elements[-1].name == "html":
            # Fragment case.
            self.parser.parse_error("unexpected-frameset-in-frameset-innerhtml")
        else:
            self.tree.open_elements.pop()
        if (not self.parser.container and
                self.tree.open_elements[-1].name != "frameset"):
            # If we're not in fragment mode and the current node is not a
            # "frameset" element (anymore) then switch.
            self.parser.phase = self.parser.phases["after frameset"]

    def end_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-end-tag-in-frameset", {"name": token["name"]})

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        ("frameset", start_tag_frameset),
        ("frame", start_tag_frame),
        ("noframes", start_tag_noframes)
    ])

    end_tag_handler = dispatch([
        ("frameset", end_tag_frameset)
    ])


class AfterFramesetPhase(Phase):
    # http://www.whatwg.org/specs/web-apps/current-work/#after3
    __slots__ = tuple()

    def process_eof(self):
        # Stop parsing
        pass

    def process_characters(self, token):
        self.parser.parse_error("unexpected-char-after-frameset")

    def start_tag_noframes(self, token):
        return self.parser.phases["in head"].process_start_tag(token)

    def start_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-start-tag-after-frameset", {"name": token["name"]})

    def end_tag_html(self, token):
        self.parser.phase = self.parser.phases["after after frameset"]

    def end_tag_other(self, token):
        self.parser.parse_error(
            "unexpected-end-tag-after-frameset", {"name": token["name"]})

    start_tag_handler = dispatch([
        ("html", Phase.start_tag_html),
        ("noframes", start_tag_noframes)
    ])

    end_tag_handler = dispatch([
        ("html", end_tag_html)
    ])


class AfterAfterBodyPhase(Phase):
    __slots__ = tuple()

    def process_eof(self):
        pass

    def process_comment(self, token):
        self.tree.insert_comment(token, self.tree.document)

    def process_space_characters(self, token):
        return self.parser.phases["in body"].process_space_characters(token)

    def process_characters(self, token):
        self.parser.parse_error("expected-eof-but-got-char")
        self.parser.phase = self.parser.phases["in body"]
        return token

    def start_tag_html(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_other(self, token):
        self.parser.parse_error(
            "expected-eof-but-got-start-tag", {"name": token["name"]})
        self.parser.phase = self.parser.phases["in body"]
        return token

    def process_end_tag(self, token):
        self.parser.parse_error(
            "expected-eof-but-got-end-tag", {"name": token["name"]})
        self.parser.phase = self.parser.phases["in body"]
        return token

    start_tag_handler = dispatch([
        ("html", start_tag_html)
    ])


class AfterAfterFramesetPhase(Phase):
    __slots__ = tuple()

    def process_eof(self):
        pass

    def process_comment(self, token):
        self.tree.insert_comment(token, self.tree.document)

    def process_space_characters(self, token):
        return self.parser.phases["in body"].process_space_characters(token)

    def process_characters(self, token):
        self.parser.parse_error("expected-eof-but-got-char")

    def start_tag_html(self, token):
        return self.parser.phases["in body"].process_start_tag(token)

    def start_tag_noframes(self, token):
        return self.parser.phases["in head"].process_start_tag(token)

    def start_tag_other(self, token):
        self.parser.parse_error(
            "expected-eof-but-got-start-tag", {"name": token["name"]})

    def process_end_tag(self, token):
        self.parser.parse_error(
            "expected-eof-but-got-end-tag", {"name": token["name"]})

    start_tag_handler = dispatch([
        ("html", start_tag_html),
        ("noframes", start_tag_noframes)
    ])


_phases = {
    "initial": InitialPhase,
    "before html": BeforeHtmlPhase,
    "before head": BeforeHeadPhase,
    "in head": InHeadPhase,
    "in head noscript": InHeadNoscriptPhase,
    "after head": AfterHeadPhase,
    "in body": InBodyPhase,
    "text": TextPhase,
    "in table": InTablePhase,
    "in table text": InTableTextPhase,
    "in caption": InCaptionPhase,
    "in column group": InColumnGroupPhase,
    "in table body": InTableBodyPhase,
    "in row": InRowPhase,
    "in cell": InCellPhase,
    "in select": InSelectPhase,
    "in select in table": InSelectInTablePhase,
    "in foreign content": InForeignContentPhase,
    "after body": AfterBodyPhase,
    "in frameset": InFramesetPhase,
    "after frameset": AfterFramesetPhase,
    "after after body": AfterAfterBodyPhase,
    "after after frameset": AfterAfterFramesetPhase,
}


def adjust_attributes(token, replacements):
    if token['data'].keys() & replacements.keys():
        token['data'] = type(token['data'])(
            (replacements.get(key, key), value) for key, value in token['data'].items())


def implied_tag_token(name, type="END_TAG", attributes=None, self_closing=False):
    return {
        "type": Token[type],
        "name": name,
        "data": {} if attributes is None else attributes,
        "selfClosing": self_closing,
    }
