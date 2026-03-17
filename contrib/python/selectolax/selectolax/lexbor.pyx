from cpython.bool cimport bool

_ENCODING = 'UTF-8'

include "base.pxi"
include "utils.pxi"
include "lexbor/attrs.pxi"
include "lexbor/node.pxi"
include "lexbor/selection.pxi"
include "lexbor/util.pxi"
include "lexbor/node_remove.pxi"

# We don't inherit from HTMLParser here, because it also includes all the C code from Modest.

cdef class LexborHTMLParser:
    """The lexbor HTML parser.

    Use this class to parse raw HTML.

    This parser mimics most of the stuff from ``HTMLParser`` but not inherits it directly.

    Parameters
    ----------

    html : str (unicode) or bytes
    """
    def __init__(self, html: str | bytes, is_fragment: bool = False):
        """Create a parser and load HTML.

        Parameters
        ----------
        html : str or bytes
            HTML content to parse.
        is_fragment : bool, optional
            When ``False`` (default), the input is parsed as a full HTML document.
            If the input is only a fragment, the parser still accepts it and inserts any missing required elements,
            (such as `<html>`, `<head>`, and `<body>`) into the tree,
            according to the HTML parsing rules in the HTML Standard.
            This matches how browsers construct the DOM when they load an HTML page.

            When ``True``, the input is parsed as an HTML fragment.
            The parser does not insert any missing required HTML elements.
            Behaves the same way as `DocumentFragment` in browsers.
            When `<html>`, `<head>` or `<body>` are present, ignores them entirely.
            As per the HTML Standard.

        """
        cdef size_t html_len
        cdef object bytes_html
        self._is_fragment = is_fragment
        self._fragment_document = NULL
        self._selector = None
        self._new_html_document()
        bytes_html, html_len = preprocess_input(html)
        self._parse_html(bytes_html, html_len)
        self.raw_html = bytes_html

    cdef inline lxb_html_document_t* main_document(self) nogil:
        if self._is_fragment:
            return self._fragment_document
        else:
            return self.document

    cdef inline void _new_html_document(self):
        """Initialize a fresh Lexbor HTML document.

        Returns
        -------
        None

        Raises
        ------
        SelectolaxError
            If the underlying Lexbor document cannot be created.
        """
        with nogil:
            self.document = lxb_html_document_create()

        if self.document == NULL:
            PyErr_SetObject(SelectolaxError, "Failed to initialize object for HTML Document.")

    cdef int _parse_html(self, char *html, size_t html_len) except -1:
        """Parse HTML content into the internal document.

        Parameters
        ----------
        html : char *
            Pointer to UTF-8 encoded HTML bytes.
        html_len : size_t
            Length of the HTML buffer.

        Returns
        -------
        int
            ``0`` on success; ``-1`` when parsing fails.

        Raises
        ------
        SelectolaxError
            If Lexbor returns a non-OK status.
        RuntimeError
            If the internal document is ``NULL`` after a successful parse.
        """
        cdef lxb_status_t status

        if self.document == NULL:
            return -1

        with nogil:
            if self._is_fragment:
                status = self._parse_html_fragment(html, html_len)
            else:
                status = self._parse_html_document(html, html_len)

        if status != LXB_STATUS_OK:
            PyErr_SetObject(SelectolaxError, "Can't parse HTML.")
            return -1

        if self.document == NULL:
            PyErr_SetObject(RuntimeError, "document is NULL even after html was parsed correctly")
            return -1
        return 0

    cdef inline lxb_status_t _parse_html_document(self, char *html, size_t html_len) nogil:
        """Parse HTML as a full HTML document.
        If the input is only a fragment, the parser still accepts it and inserts any missing required elements,
        (such as `<html>`, `<head>`, and `<body>`) into the tree,
        according to the HTML parsing rules in the HTML Standard.
        This matches how browsers construct the DOM when they load an HTML page.

        Parameters
        ----------
        html : char *
            Pointer to UTF-8 encoded HTML bytes.
        html_len : size_t
            Length of the HTML buffer.

        Returns
        -------
        lxb_status_t
            Lexbor status code produced by ``lxb_html_document_parse``.
        """
        return lxb_html_document_parse(self.document, <lxb_char_t *> html, html_len)

    cdef inline lxb_status_t _parse_html_fragment(self, char *html, size_t html_len) nogil:
        """Parse HTML as an HTML fragment.
        The parser does not insert any missing required HTML elements.

        Parameters
        ----------
        html : char *
            Pointer to UTF-8 encoded HTML bytes.
        html_len : size_t
            Length of the HTML buffer.

        Returns
        -------
        lxb_status_t
            Lexbor status code; ``LXB_STATUS_OK`` when parsing the fragment succeeded.
        """
        cdef const lxb_char_t *dummy_root_name = <const lxb_char_t *> ""
        cdef size_t dummy_root_len = 0
        cdef lxb_html_element_t *dummy_root = NULL
        cdef lxb_dom_node_t *fragment_html_node = NULL

        dummy_root = lxb_html_document_create_element(
            self.document,
            dummy_root_name,
            dummy_root_len,
            NULL
        )
        if dummy_root == NULL:
            return LXB_STATUS_ERROR
        fragment_html_node = lxb_html_document_parse_fragment(
            self.document,
            <lxb_dom_element_t *> dummy_root,
            <lxb_char_t *> html,
            html_len
        )
        if fragment_html_node == NULL:
            return LXB_STATUS_ERROR

        self._fragment_document  = <lxb_html_document_t *> fragment_html_node
        return LXB_STATUS_OK

    def __dealloc__(self):
        """Release the underlying Lexbor HTML document.

        Returns
        -------
        None

        Notes
        -----
        Safe to call multiple times; does nothing if the document is already
        freed.
        """
        if self._fragment_document != NULL:
            lxb_html_document_destroy(self._fragment_document)
        if self.document != NULL:
            lxb_html_document_destroy(self.document)

    def __repr__(self):
        """Return a concise representation of the parsed document.

        Returns
        -------
        str
            A string showing the number of characters in the parsed HTML.
        """
        html_len = len(self.root.html if self.root is not None else "")
        return f"<LexborHTMLParser chars='{html_len}'>"

    @property
    def selector(self):
        """Return a lazily created CSS selector helper.

        Returns
        -------
        LexborCSSSelector
            Selector instance bound to this parser.
        """
        if self._selector is None:
            self._selector = LexborCSSSelector()
        return self._selector

    @property
    def root(self):
        """Return the document root node.

        Returns
        -------
        LexborNode or None
            Root of the parsed document, or ``None`` if unavailable.
        """
        if self.document == NULL:
            return None
        cdef LexborNode  node
        cdef lxb_dom_node_t* dom_root
        if self._is_fragment and self._fragment_document != NULL:
            dom_root = lxb_dom_document_root(&self._fragment_document.dom_document)
        else:
            dom_root = lxb_dom_document_root(&self.document.dom_document)
        if dom_root == NULL:
            return None
        node =  LexborNode.new(dom_root, self)
        if self._is_fragment:
            node.set_as_fragment_root()
        return node

    @property
    def body(self):
        """Return document body.

        Returns
        -------
        LexborNode or None
            ``<body>`` element when present, otherwise ``None``.
        """
        cdef lxb_html_body_element_t* body
        body = lxb_html_document_body_element_noi(self.document)
        if body == NULL:
            return None
        return LexborNode.new(<lxb_dom_node_t *> body, self)

    @property
    def head(self):
        """Return document head.

        Returns
        -------
        LexborNode or None
            ``<head>`` element when present, otherwise ``None``.
        """
        cdef lxb_html_head_element_t* head
        head = lxb_html_document_head_element_noi(self.document)
        if head == NULL:
            return None
        return LexborNode.new(<lxb_dom_node_t *> head, self)

    def tags(self, str name):
        """Return all tags that match the provided name.

        Parameters
        ----------
        name : str
            Tag name to search for (e.g., ``"div"``).

        Returns
        -------
        list of LexborNode
            Matching elements in document order.

        Raises
        ------
        ValueError
            If ``name`` is empty or longer than 100 characters.
        SelectolaxError
            If Lexbor cannot locate the elements.
        """

        if not name:
            raise ValueError("Tag name cannot be empty")
        if len(name) > 100:
            raise ValueError("Tag name is too long")

        cdef lxb_dom_collection_t* collection = NULL
        cdef lxb_status_t status
        pybyte_name = name.encode('UTF-8')

        result = list()
        collection = lxb_dom_collection_make(&self.document.dom_document, 128)

        if collection == NULL:
            return result
        status = lxb_dom_elements_by_tag_name(
            <lxb_dom_element_t *> self.document,
            collection,
            <lxb_char_t *> pybyte_name,
            len(pybyte_name)
        )
        if status != 0x0000:
            lxb_dom_collection_destroy(collection, <bint> True)
            raise SelectolaxError("Can't locate elements.")

        for i in range(lxb_dom_collection_length_noi(collection)):
            node = LexborNode.new(
                <lxb_dom_node_t*> lxb_dom_collection_element_noi(collection, i),
                self
            )
            result.append(node)
        lxb_dom_collection_destroy(collection, <bint> True)
        return result

    def text(
        self,
        deep: bool = True,
        separator: str = "",
        strip: bool = False,
        skip_empty: bool = False,
    ) -> str:
        """Returns the text of the node including text of all its child nodes.

        Parameters
        ----------
        strip : bool, default False
            If true, calls ``str.strip()`` on each text part to remove extra white spaces.
        separator : str, default ''
            The separator to use when joining text from different nodes.
        deep : bool, default True
            If True, includes text from all child nodes.
        skip_empty : bool, optional
            Exclude text nodes whose content is only ASCII whitespace (space,
            tab, newline, form feed or carriage return) when ``True``.
            Defaults to ``False``.

        Returns
        -------
        text : str
            Combined textual content assembled according to the provided options.
        """
        if self.root is None:
            return ""
        return self.root.text(deep=deep, separator=separator, strip=strip, skip_empty=skip_empty)

    @property
    def html(self):
        """Return HTML representation of the page.

        Returns
        -------
        str or None
            Serialized HTML of the current document.
        """
        if self.document == NULL:
            return None
        if self._is_fragment:
            if self.root is None:
                return None
            return self.root.html
        node = LexborNode.new(<lxb_dom_node_t *> &self.document.dom_document, self)
        return node.html

    def css(self, str query):
        """A CSS selector.

        Matches pattern `query` against HTML tree.
        `CSS selectors reference <https://www.w3schools.com/cssref/css_selectors.asp>`_.

        Special selectors:

         - parser.css('p:lexbor-contains("awesome" i)') -- case-insensitive contains
         - parser.css('p:lexbor-contains("awesome")') -- case-sensitive contains

        Parameters
        ----------
        query : str
            CSS selector (e.g. "div > :nth-child(2n+1):not(:has(a))").

        Returns
        -------
        selector : list of `Node` objects
        """
        return self.root.css(query)

    def css_first(self, str query, default=None, strict=False):
        """Same as `css` but returns only the first match.

        Parameters
        ----------

        query : str
        default : Any, default None
            Default value to return if there is no match.
        strict: bool, default False
            Set to True if you want to check if there is strictly only one match in the document.


        Returns
        -------
        selector : `LexborNode` object
        """
        return self.root.css_first(query, default, strict)

    def strip_tags(self, list tags, bool recursive = False):
        """Remove specified tags from the node.

        Parameters
        ----------
        tags : list of str
            List of tags to remove.
        recursive : bool, default False
            Whenever to delete all its child nodes

        Examples
        --------

        >>> tree = LexborHTMLParser('<html><head></head><body><script></script><div>Hello world!</div></body></html>')
        >>> tags = ['head', 'style', 'script', 'xmp', 'iframe', 'noembed', 'noframes']
        >>> tree.strip_tags(tags)
        >>> tree.html
        '<html><body><div>Hello world!</div></body></html>'

        Returns
        -------
        None
        """
        cdef lxb_dom_collection_t* collection = NULL
        cdef lxb_status_t status

        for tag in tags:
            pybyte_name = tag.encode('UTF-8')

            collection = lxb_dom_collection_make(&self.document.dom_document, 128)

            if collection == NULL:
                raise SelectolaxError("Can't initialize DOM collection.")

            status = lxb_dom_elements_by_tag_name(
                <lxb_dom_element_t *> self.document,
                collection,
                <lxb_char_t *> pybyte_name,
                len(pybyte_name)
            )
            if status != 0x0000:
                lxb_dom_collection_destroy(collection, <bint> True)
                raise SelectolaxError("Can't locate elements.")

            for i in range(lxb_dom_collection_length_noi(collection)):
                if recursive:
                    lxb_dom_node_destroy_deep(<lxb_dom_node_t *> lxb_dom_collection_element_noi(collection, i))
                else:
                    lxb_dom_node_destroy(<lxb_dom_node_t *> lxb_dom_collection_element_noi(collection, i))
            lxb_dom_collection_destroy(collection, <bint> True)

    def select(self, query=None):
        """Select nodes given a CSS selector.

        Works similarly to the ``css`` method, but supports chained filtering and extra features.

        Parameters
        ----------
        query : str or None
            The CSS selector to use when searching for nodes.

        Returns
        -------
        LexborSelector or None
            Selector bound to the root node, or ``None`` if the document is empty.
        """
        cdef LexborNode node
        node = self.root
        if node:
            return LexborSelector(node, query)
        return None

    def any_css_matches(self, tuple selectors):
        """Return ``True`` if any of the specified CSS selectors match.

        Parameters
        ----------
        selectors : tuple[str]
            CSS selectors to evaluate.

        Returns
        -------
        bool
            ``True`` when at least one selector matches.
        """
        return self.root.any_css_matches(selectors)

    def scripts_contain(self, str query):
        """Return ``True`` if any script tag contains the given text.

        Caches script tags on the first call to improve performance.

        Parameters
        ----------
        query : str
            Text to search for within script contents.

        Returns
        -------
        bool
            ``True`` when a matching script tag is found.
        """
        return self.root.scripts_contain(query)

    def script_srcs_contain(self, tuple queries):
        """Return ``True`` if any script ``src`` contains one of the strings.

        Caches values on the first call to improve performance.

        Parameters
        ----------
        queries : tuple of str
            Strings to look for inside ``src`` attributes.

        Returns
        -------
        bool
            ``True`` when a matching source value is found.
        """
        return self.root.script_srcs_contain(queries)

    def css_matches(self, str selector):
        """Return ``True`` if the document matches the selector at least once.

        Parameters
        ----------
        selector : str
            CSS selector to test.

        Returns
        -------
        bool
            ``True`` when a match exists.
        """
        return self.root.css_matches(selector)

    def merge_text_nodes(self):
        """Iterates over all text nodes and merges all text nodes that are close to each other.

        This is useful for text extraction.
        Use it when you need to strip HTML tags and merge "dangling" text.

        Examples
        --------

        >>> tree = LexborHTMLParser("<div><p><strong>J</strong>ohn</p><p>Doe</p></div>")
        >>> node = tree.css_first('div')
        >>> tree.unwrap_tags(["strong"])
        >>> tree.text(deep=True, separator=" ", strip=True)
        "J ohn Doe" # Text extraction produces an extra space because the strong tag was removed.
        >>> node.merge_text_nodes()
        >>> tree.text(deep=True, separator=" ", strip=True)
        "John Doe"

        Returns
        -------
        None
        """
        return self.root.merge_text_nodes()

    @staticmethod
    cdef LexborHTMLParser from_document(lxb_html_document_t *document, bytes raw_html):
        """Construct a parser from an existing Lexbor document.

        Parameters
        ----------
        document : lxb_html_document_t *
            Borrowed pointer to an initialized Lexbor HTML document.
        raw_html : bytes
            Original HTML bytes backing the document.

        Returns
        -------
        LexborHTMLParser
            Parser instance wrapping the provided document.
        """
        obj = <LexborHTMLParser> LexborHTMLParser.__new__(LexborHTMLParser)
        obj.document = document
        obj.raw_html = raw_html
        obj.cached_script_texts = None
        obj.cached_script_srcs = None
        obj._selector = None
        return obj

    def clone(self):
        """Clone the current document tree.

        You can use to do temporary modifications without affecting the original HTML tree.
        It is tied to the current parser instance.
        Gets destroyed when the parser instance is destroyed.

        Returns
        -------
        LexborHTMLParser
            A parser instance backed by a deep-copied document.
        """
        cdef lxb_html_document_t* cloned_document
        cdef lxb_dom_node_t* cloned_node
        cdef LexborHTMLParser cls

        with nogil:
            cloned_document = lxb_html_document_create()

        if cloned_document == NULL:
            raise SelectolaxError("Can't create a new document")

        cloned_document.ready_state = LXB_HTML_DOCUMENT_READY_STATE_COMPLETE

        with nogil:
            cloned_node = lxb_dom_document_import_node(
                &cloned_document.dom_document,
                <lxb_dom_node_t *> lxb_dom_document_root(&self.main_document().dom_document),
                <bint> True
            )

        if cloned_node == NULL:
            raise SelectolaxError("Can't create a new document")

        with nogil:
            lxb_dom_node_insert_child(<lxb_dom_node_t * > cloned_document, cloned_node)

        cls = LexborHTMLParser.from_document(cloned_document, self.raw_html)
        return cls

    def unwrap_tags(self, list tags, delete_empty = False):
        """Unwraps specified tags from the HTML tree.

        Works the same as the ``unwrap`` method, but applied to a list of tags.

        Parameters
        ----------
        tags : list
            List of tags to remove.
        delete_empty : bool
            Whenever to delete empty tags.

        Examples
        --------

        >>> tree = LexborHTMLParser("<div><a href="">Hello</a> <i>world</i>!</div>")
        >>> tree.body.unwrap_tags(['i','a'])
        >>> tree.body.html
        '<body><div>Hello world!</div></body>'

        Returns
        -------
        None
        """
        # faster to check if the document is empty which should determine if we have a root
        if self.document != NULL:
            self.root.unwrap_tags(tags, delete_empty=delete_empty)

    @property
    def inner_html(self) -> str:
        """Return HTML representation of the child nodes.

        Works similar to innerHTML in JavaScript.
        Unlike the `.html` property, does not include the current node.
        Can be used to set HTML as well. See the setter docstring.

        Returns
        -------
        text : str | None
        """
        return self.root.inner_html

    @inner_html.setter
    def inner_html(self, str html):
        """Set inner HTML to the specified HTML.

        Replaces existing data inside the node.
        Works similar to innerHTML in JavaScript.

        Parameters
        ----------
        html : str

        Returns
        -------
        None
        """
        self.root.inner_html = html

    def create_node(self, str tag):
        """Given an HTML tag name, e.g. `"div"`, create a single empty node for that tag,
        e.g. `"<div></div>"`.

        Parameters
        ----------
        tag : str
            Name of the tag to create.

        Returns
        -------
        LexborNode
            Newly created element node.
        Raises
        ------
        SelectolaxError
            If the element cannot be created.

        Examples
        --------
        >>> parser = LexborHTMLParser("<div></div>")
        >>> new_node = parser.create_node("span")
        >>> new_node.tag_name
        'span'
        >>> parser.root.append_child(new_node)
        >>> parser.html
        '<html><head></head><body><div><span></span></div></body></html>'
        """
        cdef lxb_html_element_t* element
        cdef lxb_dom_node_t* dom_node
        if not tag:
            raise SelectolaxError("Tag name cannot be empty")
        pybyte_name = tag.encode('UTF-8')

        element = lxb_html_document_create_element(
            self.document,
            <const lxb_char_t *> pybyte_name,
            len(pybyte_name),
            NULL
        )

        if element == NULL:
            raise SelectolaxError(f"Can't create element for tag '{tag}'")

        dom_node = <lxb_dom_node_t *> element

        return LexborNode.new(dom_node, self)
