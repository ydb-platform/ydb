cimport cython
from cpython.exc cimport PyErr_SetNone

import logging

logger = logging.getLogger("selectolax")

_TAG_TO_NAME = {
    0x0005: "-doctype",
    0x0002: "-text",
    0x0004: "-comment",
}
ctypedef fused str_or_LexborNode:
    str
    bytes
    LexborNode

ctypedef fused str_or_bytes:
    str
    bytes

cdef inline bytes to_bytes(str_or_LexborNode value):
    cdef bytes bytes_val
    if isinstance(value, unicode):
        bytes_val = <bytes> value.encode("utf-8")
    elif isinstance(value, bytes):
        bytes_val = <bytes> value
    return bytes_val


@cython.final
cdef class LexborNode:
    """A class that represents HTML node (element)."""

    cdef void set_as_fragment_root(self):
        self._is_fragment_root = 1

    @staticmethod
    cdef LexborNode new(lxb_dom_node_t *node, LexborHTMLParser parser):
        cdef LexborNode lxbnode = LexborNode.__new__(LexborNode)
        lxbnode.node = node
        lxbnode.parser = parser
        lxbnode._is_fragment_root = 0
        return lxbnode

    @property
    def mem_id(self):
        return <size_t> self.node

    @property
    def child(self):
        """Alias for the `first_child` property.

        **Deprecated**. Please use `first_child` instead.
        """
        return self.first_child

    @property
    def first_child(self):
        """Return the first child node."""
        cdef LexborNode node
        if self.node.first_child:
            node = LexborNode.new(<lxb_dom_node_t *> self.node.first_child, self.parser)
            return node
        return None

    @property
    def parent(self):
        """Return the parent node."""
        cdef LexborNode node
        if self.node.parent != NULL:
            node = LexborNode.new(<lxb_dom_node_t *> self.node.parent, self.parser)
            return node
        return None

    @property
    def next(self):
        """Return next node."""
        cdef LexborNode node
        if self.node.next != NULL:
            node = LexborNode.new(<lxb_dom_node_t *> self.node.next, self.parser)
            return node
        return None

    @property
    def prev(self):
        """Return previous node."""
        cdef LexborNode node
        if self.node.prev != NULL:
            node = LexborNode.new(<lxb_dom_node_t *> self.node.prev, self.parser)
            return node
        return None

    @property
    def last_child(self):
        """Return last child node."""
        cdef LexborNode node
        if self.node.last_child != NULL:
            node = LexborNode.new(<lxb_dom_node_t *> self.node.last_child, self.parser)
            return node
        return None

    @property
    def html(self):
        """Return HTML representation of the current node including all its child nodes.

        Returns
        -------
        text : str
        """
        cdef lexbor_str_t *lxb_str
        cdef lxb_status_t status

        lxb_str = lexbor_str_create()
        if self._is_fragment_root:
            status = serialize_fragment(self.node, lxb_str)
            # status = lxb_html_serialize_tree_str(self.node, lxb_str)
        else:
            status = lxb_html_serialize_tree_str(self.node, lxb_str)
        if status == 0 and lxb_str.data:
            html = lxb_str.data.decode(_ENCODING).replace('<-undef>', '')
            lexbor_str_destroy(lxb_str, self.node.owner_document.text, True)
            return html
        return None

    def __hash__(self):
        return self.mem_id

    def text_lexbor(self):
        """Returns the text of the node including text of all its child nodes.

        Uses builtin method from lexbor.
        """

        cdef size_t str_len = 0
        cdef lxb_char_t * text

        text = lxb_dom_node_text_content(self.node, &str_len)
        if <int> str_len == 0:
            raise RuntimeError("Can't extract text")

        unicode_text = text.decode(_ENCODING)
        lxb_dom_document_destroy_text_noi(self.node.owner_document, text)
        return unicode_text

    def text(self, bool deep=True, str separator='', bool strip=False, bool skip_empty=False):
        """Return concatenated text from this node.

        Parameters
        ----------
        deep : bool, optional
            When ``True`` (default), include text from all descendant nodes; when
            ``False``, only include direct children.
        separator : str, optional
            String inserted between successive text fragments.
        strip : bool, optional
            If ``True``, apply ``str.strip()`` to each fragment before joining to
            remove surrounding whitespace. Defaults to ``False``.
        skip_empty : bool, optional
            Exclude text nodes whose content is only ASCII whitespace (space,
            tab, newline, form feed or carriage return) when ``True``.
            Defaults to ``False``.

        Returns
        -------
        text : str
            Combined textual content assembled according to the provided options.

        """
        cdef unsigned char * text
        cdef lxb_dom_node_t * node = <lxb_dom_node_t *> self.node.first_child

        if not deep:
            container = TextContainer(separator, strip)
            if _is_node_type(self.node, LXB_DOM_NODE_TYPE_TEXT):
                text = <unsigned char *> lexbor_str_data_noi(&(<lxb_dom_character_data_t *> self.node).data)
                if text != NULL:
                    if not skip_empty or not self.is_empty_text_node:
                        py_text = text.decode(_ENCODING)
                        container.append(py_text)

            while node != NULL:
                if _is_node_type(node, LXB_DOM_NODE_TYPE_TEXT):
                    text = <unsigned char *> lexbor_str_data_noi(&(<lxb_dom_character_data_t *> node).data)
                    if text != NULL:
                        if not skip_empty or not is_empty_text_node(node):
                            py_text = text.decode(_ENCODING)
                            container.append(py_text)
                node = node.next
            return container.text
        else:
            container = TextContainer(separator, strip)
            if _is_node_type(self.node, LXB_DOM_NODE_TYPE_TEXT):
                text = <unsigned char *> lexbor_str_data_noi(&(<lxb_dom_character_data_t *> self.node).data)
                if text != NULL:
                    if not skip_empty or not self.is_empty_text_node:
                        container.append(text.decode(_ENCODING))

            lxb_dom_node_simple_walk(
                <lxb_dom_node_t *> self.node,
                <lxb_dom_node_simple_walker_f> text_callback,
                <void *> container
            )
            return container.text

    cdef inline LexborNode _get_node(self):
        cdef LexborNode node
        if self._is_fragment_root:
            node = self.parent
        else:
            node = self
        return node

    def css(self, str query):
        """Evaluate CSS selector against current node and its child nodes.

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
        return self.parser.selector.find(query, self._get_node())

    def css_first(self, str query, default=None, bool strict=False):
        """Same as `css` but returns only the first match.

        When `strict=False` stops at the first match. Works faster.

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
        if strict:
            results = self.parser.selector.find(query, self._get_node())
        else:
            results = self.parser.selector.find_first(query, self._get_node())
        n_results = len(results)
        if n_results > 0:
            if strict and n_results > 1:
                raise ValueError("Expected 1 match, but found %s matches" % n_results)
            return results[0]
        return default

    def any_css_matches(self, tuple selectors):
        """Returns True if any of CSS selectors matches a node"""
        for selector in selectors:
            if self.parser.selector.any_matches(selector, self):
                return True
        return False

    def css_matches(self, str selector):
        """Returns True if CSS selector matches a node."""
        return bool(self.parser.selector.any_matches(selector, self))

    def __repr__(self):
        return '<LexborNode %s>' % self.tag

    @property
    def tag_id(self):
        cdef lxb_tag_id_t tag_id = lxb_dom_node_tag_id_noi(self.node)
        return tag_id

    @property
    def tag(self):
        """Return the name of the current tag (e.g. div, p, img).

        For for non-tag nodes, returns the following names:

         * `-text` - text node
         * `-document` - document node
         * `-comment` - comment node

        This

        Returns
        -------
        text : str
        """

        cdef lxb_char_t *c_text
        cdef size_t str_len = 0
        if self.tag_id in [LXB_TAG__EM_DOCTYPE, LXB_TAG__TEXT, LXB_TAG__EM_COMMENT]:
            return _TAG_TO_NAME[self.tag_id]
        c_text = lxb_dom_element_qualified_name(<lxb_dom_element_t *> self.node, &str_len)
        text = None
        if c_text:
            text = c_text.decode(_ENCODING)
        return text

    def decompose(self, bool recursive=True):
        """Remove the current node from the tree.

        Parameters
        ----------
        recursive : bool, default True
            Whenever to delete all its child nodes

        Examples
        --------

        >>> tree = LexborHTMLParser(html)
        >>> for tag in tree.css('script'):
        >>>     tag.decompose()

        """
        if self.node == <lxb_dom_node_t *> lxb_dom_document_root(&self.parser.document.dom_document):
            raise SelectolaxError("Decomposing the root node is not allowed.")

        if recursive:
            node_remove_deep(<lxb_dom_node_t *> self.node)
        else:
            lxb_dom_node_remove(<lxb_dom_node_t *> self.node)

    def strip_tags(self, list tags, bool recursive = False):
        """Remove specified tags from the HTML tree.

        Parameters
        ----------
        tags : list
            List of tags to remove.
        recursive : bool, default True
            Whenever to delete all its child nodes

        Examples
        --------

        >>> tree = LexborHTMLParser('<html><head></head><body><script></script><div>Hello world!</div></body></html>')
        >>> tags = ['head', 'style', 'script', 'xmp', 'iframe', 'noembed', 'noframes']
        >>> tree.strip_tags(tags)
        >>> tree.html
        '<html><body><div>Hello world!</div></body></html>'

        """
        cdef LexborNode element
        for tag in tags:
            for element in self.css(tag):
                element.decompose(recursive=recursive)

    @property
    def attributes(self):
        """Get all attributes that belong to the current node.

        The value of empty attributes is None.

        Returns
        -------
        attributes : dictionary of all attributes.

        Examples
        --------

        >>> tree = LexborHTMLParser("<div data id='my_id'></div>")
        >>> node = tree.css_first('div')
        >>> node.attributes
        {'data': None, 'id': 'my_id'}
        """
        cdef lxb_dom_attr_t *attr = lxb_dom_element_first_attribute_noi(<lxb_dom_element_t *> self.node)
        cdef size_t str_len = 0
        attributes = dict()

        if not _is_node_type(self.node, LXB_DOM_NODE_TYPE_ELEMENT):
            return attributes

        while attr != NULL:
            key = lxb_dom_attr_local_name_noi(attr, &str_len)
            value = lxb_dom_attr_value_noi(attr, &str_len)

            if value:
                py_value = value.decode(_ENCODING)
            else:
                py_value = None
            attributes[key.decode(_ENCODING)] = py_value

            attr = attr.next
        return attributes

    @property
    def attrs(self):
        """A dict-like object that is similar to the ``attributes`` property, but operates directly on the Node data.

        .. warning:: Use ``attributes`` instead, if you don't want to modify Node attributes.

        Returns
        -------
        attributes : Attributes mapping object.

        Examples
        --------

        >>> tree = LexborHTMLParser("<div id='a'></div>")
        >>> node = tree.css_first('div')
        >>> node.attrs
        <div attributes, 1 items>
        >>> node.attrs['id']
        'a'
        >>> node.attrs['foo'] = 'bar'
        >>> del node.attrs['id']
        >>> node.attributes
        {'foo': 'bar'}
        >>> node.attrs['id'] = 'new_id'
        >>> node.html
        '<div foo="bar" id="new_id"></div>'
        """
        cdef LexborAttributes attributes = LexborAttributes.create(<lxb_dom_node_t *> self.node)
        return attributes

    @property
    def id(self):
        """Get the id attribute of the node.

        Returns None if id does not set.

        Returns
        -------
        text : str
        """
        cdef char * key = 'id'
        cdef size_t str_len
        cdef lxb_dom_attr_t * attr = lxb_dom_element_attr_by_name(
            <lxb_dom_element_t *> self.node,
            <lxb_char_t *> key, 2
        )
        if attr != NULL:
            value = lxb_dom_attr_value_noi(attr, &str_len)
            return value.decode(_ENCODING) if value else None
        return None

    def iter(self, bool include_text = False, bool skip_empty = False):
        """Iterate over direct children of this node.

        Parameters
        ----------
        include_text : bool, optional
            When ``True``, yield text nodes in addition to element nodes. Defaults
            to ``False``.
        skip_empty : bool, optional
            When ``include_text`` is ``True``, ignore text nodes made up solely
            of ASCII whitespace (space, tab, newline, form feed or carriage
            return). Defaults to ``False``.

        Yields
        ------
        LexborNode
            Child nodes on the same tree level as this node, filtered according
            to the provided options.
        """

        cdef lxb_dom_node_t *node = self.node.first_child
        cdef LexborNode next_node

        while node != NULL:
            if node.type == LXB_DOM_NODE_TYPE_TEXT and not include_text:
                node = node.next
                continue
            if node.type == LXB_DOM_NODE_TYPE_TEXT and include_text and skip_empty and is_empty_text_node(node):
                node = node.next
                continue

            next_node = LexborNode.new(<lxb_dom_node_t *> node, self.parser)
            yield next_node
            node = node.next

    def __iter__(self):
        return self.iter()

    def __next__(self):
        return self.next

    def unwrap(self, bint delete_empty=False):
        """Replace node with whatever is inside this node.

        Does nothing if you perform unwrapping second time on the same node.

        Parameters
        ----------
        delete_empty : bool, default False
            If True, removes empty tags.

        Examples
        --------

        >>>  tree = LexborHTMLParser("<div>Hello <i>world</i>!</div>")
        >>>  tree.css_first('i').unwrap()
        >>>  tree.html
        '<html><head></head><body><div>Hello world!</div></body></html>'

        Note: by default, empty tags are ignored, use "delete_empty" to change this.
        """

        if node_is_removed(<lxb_dom_node_t *> self.node) == 1:
            logger.error("Attempt to unwrap removed node. Does nothing.")
            return

        if self.node.first_child == NULL:
            if delete_empty:
                lxb_dom_node_remove(<lxb_dom_node_t *> self.node)
            return
        cdef lxb_dom_node_t * next_node
        cdef lxb_dom_node_t * current_node

        if self.node.first_child.next != NULL:
            current_node = self.node.first_child
            next_node = current_node.next

            while next_node != NULL:
                next_node = current_node.next
                lxb_dom_node_insert_before(self.node, current_node)
                current_node = next_node
        else:
            lxb_dom_node_insert_before(self.node, self.node.first_child)
        lxb_dom_node_remove(<lxb_dom_node_t *> self.node)

    def unwrap_tags(self, list tags, bint delete_empty = False):
        """Unwraps specified tags from the HTML tree.

        Works the same as the ``unwrap`` method, but applied to a list of tags.

        Parameters
        ----------
        tags : list
            List of tags to remove.
        delete_empty : bool, default False
            If True, removes empty tags.

        Examples
        --------

        >>> tree = LexborHTMLParser("<div><a href="">Hello</a> <i>world</i>!</div>")
        >>> tree.body.unwrap_tags(['i','a'])
        >>> tree.body.html
        '<body><div>Hello world!</div></body>'

        Note: by default, empty tags are ignored, use "delete_empty" to change this.
        """
        cdef LexborNode element
        for tag in tags:
            for element in self.css(tag):
                element.unwrap(delete_empty)

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
        """
        cdef lxb_dom_node_t *node = self.node.first_child
        cdef lxb_dom_node_t *next_node
        cdef lxb_char_t *left_text
        cdef lxb_char_t *right_text
        cdef size_t left_length, right_length

        while node != NULL:
            next_node = node.next
            if node.type == LXB_DOM_NODE_TYPE_TEXT and node.prev and node.prev.type == LXB_DOM_NODE_TYPE_TEXT:
                left_text = lxb_dom_node_text_content(node.prev, &left_length)
                right_text = lxb_dom_node_text_content(node, &right_length)
                if left_text and right_text:
                    combined = (<bytes> left_text[:left_length]) + (<bytes> right_text[:right_length])
                    lxb_dom_node_text_content_set(node, combined, len(combined))
                    lxb_dom_node_remove(node.prev)

                if left_text is not NULL:
                    lxb_dom_document_destroy_text_noi(self.node.owner_document, left_text)
                if right_text is not NULL:
                    lxb_dom_document_destroy_text_noi(self.node.owner_document, right_text)

            if node.first_child:
                LexborNode.new(node, self.parser).merge_text_nodes()
            node = next_node

    def traverse(self, bool include_text = False, bool skip_empty = False):
        """Depth-first traversal starting at the current node.

        Parameters
        ----------
        include_text : bool, optional
            When ``True``, include text nodes in the traversal sequence. Defaults
            to ``False``.
        skip_empty : bool, optional
            Skip text nodes that contain only ASCII whitespace (space, tab,
            newline, form feed or carriage return) when ``include_text`` is
            ``True``. Defaults to ``False``.

        Yields
        ------
        LexborNode
            Nodes encountered in depth-first order beginning with the current
            node, filtered according to the provided options.
        """
        cdef lxb_dom_node_t * root = self.node
        cdef lxb_dom_node_t * node = root
        cdef LexborNode lxb_node

        while node != NULL:
            if include_text or node.type != LXB_DOM_NODE_TYPE_TEXT:
                if not skip_empty or not is_empty_text_node(node):
                    lxb_node = LexborNode.new(<lxb_dom_node_t *> node, self.parser)
                    yield lxb_node

            if node.first_child != NULL:
                node = node.first_child
            else:
                while node != root and node.next == NULL:
                    node = node.parent
                if node == root:
                    break
                node = node.next

    def replace_with(self, str_or_LexborNode value):
        """Replace current Node with specified value.

        Parameters
        ----------
        value : str, bytes or Node
            The text or Node instance to replace the Node with.
            When a text string is passed, it's treated as text. All HTML tags will be escaped.
            Convert and pass the ``Node`` object when you want to work with HTML.
            Does not clone the ``Node`` object.
            All future changes to the passed ``Node`` object will also be taken into account.

        Examples
        --------

        >>> tree = LexborHTMLParser('<div>Get <img src="" alt="Laptop"></div>')
        >>> img = tree.css_first('img')
        >>> img.replace_with(img.attributes.get('alt', ''))
        >>> tree.body.child.html
        '<div>Get Laptop</div>'

        >>> html_parser = LexborHTMLParser('<div>Get <span alt="Laptop"><img src="/jpg"> <div></div></span></div>')
        >>> html_parser2 = LexborHTMLParser('<div>Test</div>')
        >>> img_node = html_parser.css_first('img')
        >>> img_node.replace_with(html_parser2.body.child)
        '<div>Get <span alt="Laptop"><div>Test</div> <div></div></span></div>'
        """
        cdef lxb_dom_node_t * new_node

        if isinstance(value, (str, bytes, unicode)):
            bytes_val = to_bytes(value)
            new_node = <lxb_dom_node_t *> lxb_dom_document_create_text_node(
                &self.parser.document.dom_document,
                <lxb_char_t *> bytes_val, len(bytes_val)
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_before(self.node, new_node)
            lxb_dom_node_remove(<lxb_dom_node_t *> self.node)
        elif isinstance(value, LexborNode):
            new_node = lxb_dom_document_import_node(
                &self.parser.document.dom_document,
                <lxb_dom_node_t *> value.node,
                <bint> True
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_before(self.node, <lxb_dom_node_t *> new_node)
            lxb_dom_node_remove(<lxb_dom_node_t *> self.node)
        else:
            raise SelectolaxError("Expected a string or LexborNode instance, but %s found" % type(value).__name__)

    def insert_before(self, str_or_LexborNode value):
        """
        Insert a node before the current Node.

        Parameters
        ----------
        value : str, bytes or Node
            The text or Node instance to insert before the Node.
            When a text string is passed, it's treated as text. All HTML tags will be escaped.
            Convert and pass the ``Node`` object when you want to work with HTML.
            Does not clone the ``Node`` object.
            All future changes to the passed ``Node`` object will also be taken into account.

        Examples
        --------

        >>> tree = LexborHTMLParser('<div>Get <img src="" alt="Laptop"></div>')
        >>> img = tree.css_first('img')
        >>> img.insert_before(img.attributes.get('alt', ''))
        >>> tree.body.child.html
        '<div>Get Laptop<img src="" alt="Laptop"></div>'

        >>> html_parser = LexborHTMLParser('<div>Get <span alt="Laptop"><img src="/jpg"> <div></div></span></div>')
        >>> html_parser2 = LexborHTMLParser('<div>Test</div>')
        >>> img_node = html_parser.css_first('img')
        >>> img_node.insert_before(html_parser2.body.child)
        <div>Get <span alt="Laptop"><div>Test</div><img src="/jpg"> <div></div></span></div>'
        """
        cdef lxb_dom_node_t * new_node

        if isinstance(value, (str, bytes, unicode)):
            bytes_val = to_bytes(value)
            new_node = <lxb_dom_node_t *> lxb_dom_document_create_text_node(
                &self.parser.document.dom_document,
                <lxb_char_t *> bytes_val, len(bytes_val)
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_before(self.node, new_node)
        elif isinstance(value, LexborNode):
            new_node = lxb_dom_document_import_node(
                &self.parser.document.dom_document,
                <lxb_dom_node_t *> value.node,
                <bint> True
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_before(self.node, <lxb_dom_node_t *> new_node)
        else:
            raise SelectolaxError("Expected a string or LexborNode instance, but %s found" % type(value).__name__)

    def insert_after(self, str_or_LexborNode value):
        """
        Insert a node after the current Node.

        Parameters
        ----------
        value : str, bytes or Node
            The text or Node instance to insert after the Node.
            When a text string is passed, it's treated as text. All HTML tags will be escaped.
            Convert and pass the ``Node`` object when you want to work with HTML.
            Does not clone the ``Node`` object.
            All future changes to the passed ``Node`` object will also be taken into account.

        Examples
        --------

        >>> tree = LexborHTMLParser('<div>Get <img src="" alt="Laptop"></div>')
        >>> img = tree.css_first('img')
        >>> img.insert_after(img.attributes.get('alt', ''))
        >>> tree.body.child.html
        '<div>Get <img src="" alt="Laptop">Laptop</div>'

        >>> html_parser = LexborHTMLParser('<div>Get <span alt="Laptop"><img src="/jpg"> <div></div></span></div>')
        >>> html_parser2 = LexborHTMLParser('<div>Test</div>')
        >>> img_node = html_parser.css_first('img')
        >>> img_node.insert_after(html_parser2.body.child)
        <div>Get <span alt="Laptop"><img src="/jpg"><div>Test</div> <div></div></span></div>'
        """
        cdef lxb_dom_node_t * new_node

        if isinstance(value, (str, bytes, unicode)):
            bytes_val = to_bytes(value)
            new_node = <lxb_dom_node_t *> lxb_dom_document_create_text_node(
                &self.parser.document.dom_document,
                <lxb_char_t *> bytes_val, len(bytes_val)
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_after(self.node, new_node)
        elif isinstance(value, LexborNode):
            new_node = lxb_dom_document_import_node(
                &self.parser.document.dom_document,
                <lxb_dom_node_t *> value.node,
                <bint> True
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_after(self.node, <lxb_dom_node_t *> new_node)
        else:
            raise SelectolaxError("Expected a string or LexborNode instance, but %s found" % type(value).__name__)

    def insert_child(self, str_or_LexborNode value):
        """
        Insert a node inside (at the end of) the current Node.

        Parameters
        ----------
        value : str, bytes or Node
            The text or Node instance to insert inside the Node.
            When a text string is passed, it's treated as text. All HTML tags will be escaped.
            Convert and pass the ``Node`` object when you want to work with HTML.
            Does not clone the ``Node`` object.
            All future changes to the passed ``Node`` object will also be taken into account.

        Examples
        --------

        >>> tree = LexborHTMLParser('<div>Get <img src=""></div>')
        >>> div = tree.css_first('div')
        >>> div.insert_child('Laptop')
        >>> tree.body.child.html
        '<div>Get <img src="">Laptop</div>'

        >>> html_parser = LexborHTMLParser('<div>Get <span alt="Laptop"> <div>Laptop</div> </span></div>')
        >>> html_parser2 = LexborHTMLParser('<div>Test</div>')
        >>> span_node = html_parser.css_first('span')
        >>> span_node.insert_child(html_parser2.body.child)
        <div>Get <span alt="Laptop"> <div>Laptop</div> <div>Test</div> </span></div>'
        """
        cdef lxb_dom_node_t * new_node

        if isinstance(value, (str, bytes, unicode)):
            bytes_val = to_bytes(value)
            new_node = <lxb_dom_node_t *> lxb_dom_document_create_text_node(
                &self.parser.document.dom_document,
                <lxb_char_t *> bytes_val, len(bytes_val)
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_child(self.node, new_node)
        elif isinstance(value, LexborNode):
            new_node = lxb_dom_document_import_node(
                &self.parser.document.dom_document,
                <lxb_dom_node_t *> value.node,
                <bint> True
            )
            if new_node == NULL:
                raise SelectolaxError("Can't create a new node")
            lxb_dom_node_insert_child(self.node, <lxb_dom_node_t *> new_node)
        else:
            raise SelectolaxError("Expected a string or LexborNode instance, but %s found" % type(value).__name__)

    @property
    def raw_value(self):
        """Return the raw (unparsed, original) value of a node.

        Currently, works on text nodes only.

        Returns
        -------

        raw_value : bytes

        Examples
        --------

        >>> html_parser = LexborHTMLParser('<div>&#x3C;test&#x3E;</div>')
        >>> selector = html_parser.css_first('div')
        >>> selector.child.html
        '&lt;test&gt;'
        >>> selector.child.raw_value
        b'&#x3C;test&#x3E;'
        """
        raise NotImplementedError("This features is not supported by the lexbor backend. Please use Modest backend.")

    def scripts_contain(self, str query):
        """Returns True if any of the script tags contain specified text.

        Caches script tags on the first call to improve performance.

        Parameters
        ----------
        query : str
            The query to check.

        """
        cdef LexborNode node
        if self.parser.cached_script_texts is None:
            nodes = self.parser.selector.find('script', self)
            text_nodes = []
            for node in nodes:
                node_text = node.text(deep=True)
                if node_text:
                    text_nodes.append(node_text)
            self.parser.cached_script_texts = text_nodes

        for text in self.parser.cached_script_texts:
            if query in text:
                return True
        return False

    def script_srcs_contain(self, tuple queries):
        """Returns True if any of the script SRCs attributes contain on of the specified text.

        Caches values on the first call to improve performance.

        Parameters
        ----------
        queries : tuple of str

        """
        cdef LexborNode node
        if self.parser.cached_script_srcs is None:
            nodes = self.parser.selector.find('script', self)
            src_nodes = []
            for node in nodes:
                node_src = node.attrs.get('src')
                if node_src:
                    src_nodes.append(node_src)
            self.parser.cached_script_srcs = src_nodes

        for text in self.parser.cached_script_srcs:
            for query in queries:
                if query in text:
                    return True
        return False

    def remove(self, bool recursive=True):
        """An alias for the decompose method."""
        self.decompose(recursive)

    def select(self, query=None):
        """Select nodes given a CSS selector.

        Works similarly to the the ``css`` method, but supports chained filtering and extra features.

        Parameters
        ----------
        query : str or None
            The CSS selector to use when searching for nodes.

        Returns
        -------
        selector : The `Selector` class.
        """
        return LexborSelector(self._get_node(), query)

    def __eq__(self, other):
        if isinstance(other, str):
            return self.html == other
        if not isinstance(other, LexborNode):
            return False
        return self.html == other.html

    @property
    def text_content(self):
        """Returns the text of the node if it is a text node.

        Returns None for other nodes.
        Unlike the ``text`` method, does not include child nodes.

        Returns
        -------
        text : str or None.
        """
        cdef unsigned char * text
        cdef lxb_dom_node_t * node = <lxb_dom_node_t *> self.node.first_child
        cdef TextContainer container
        if not _is_node_type(self.node, LXB_DOM_NODE_TYPE_TEXT):
            return None

        text = <unsigned char *> lexbor_str_data_noi(&(<lxb_dom_character_data_t *> self.node).data)
        if text != NULL:
            container = TextContainer.new_with_defaults()
            py_text = text.decode(_ENCODING)
            container.append(py_text)
            return container.text
        return None

    @property
    def comment_content(self) -> str | None:
        """Extract the textual content of an HTML comment node.

        Returns
        -------
        str or None
            Comment text with surrounding whitespace removed, or ``None`` if
            the current node is not a comment or the comment markup cannot be
            parsed.

        Examples
        --------
        >>> parse_fragment("<!-- hello -->")[0].comment_content
        'hello'
        >>> parse_fragment("<div>not a comment</div>")[0].comment_content is None
        True
        """
        if not self.is_comment_node:
            return None
        try:
            return extract_html_comment(self.html)
        except (ValueError, AttributeError, IndexError):
            return None

    @property
    def inner_html(self) -> str | None:
        """Return HTML representation of the child nodes.

        Works similar to innerHTML in JavaScript.
        Unlike the `.html` property, does not include the current node.
        Can be used to set HTML as well. See the setter docstring.

        Returns
        -------
        text : str | None
        """

        cdef lexbor_str_t *lxb_str
        cdef lxb_status_t status

        lxb_str = lexbor_str_create()
        status = lxb_html_serialize_deep_str(self.node, lxb_str)
        if status == 0 and lxb_str.data:
            html = lxb_str.data.decode(_ENCODING).replace('<-undef>', '')
            lexbor_str_destroy(lxb_str, self.node.owner_document.text, True)
            return html
        return None

    @inner_html.setter
    def inner_html(self, str html) -> None:
        """Set inner HTML to the specified HTML.

        Replaces existing data inside the node.
        Works similar to innerHTML in JavaScript.

        Parameters
        ----------
        html : str | None

        """
        cdef bytes bytes_val
        bytes_val = <bytes> html.encode("utf-8")
        lxb_html_element_inner_html_set(
            <lxb_html_element_t *> self.node,
            <lxb_char_t *> bytes_val, len(bytes_val)
        )

    def clone(self) -> LexborNode:
        """Clone the current node.

        You can use to do temporary modifications without affecting the original HTML tree.

        It is tied to the current parser instance.
        Gets destroyed when parser instance is destroyed.
        """
        cdef lxb_dom_node_t * node
        node = lxb_dom_node_clone(<lxb_dom_node_t *> self.node, 1)
        return LexborNode.new(node, self.parser)

    @property
    def is_element_node(self) -> bool:
        """Return True if the node represents an element node."""
        return _is_node_type(self.node, LXB_DOM_NODE_TYPE_ELEMENT)

    @property
    def is_text_node(self) -> bool:
        """Return True if the node represents a text node."""
        return _is_node_type(self.node, LXB_DOM_NODE_TYPE_TEXT)

    @property
    def is_comment_node(self) -> bool:
        """Return True if the node represents a comment node."""
        return _is_node_type(self.node, LXB_DOM_NODE_TYPE_COMMENT)

    @property
    def is_document_node(self) -> bool:
        """Return True if the node represents a document node."""
        return _is_node_type(self.node, LXB_DOM_NODE_TYPE_DOCUMENT)

    @property
    def is_empty_text_node(self) -> bool:
        """Check whether the current node is an empty text node.

        Returns
        -------
        bool
            ``True`` when the node is a text node whose character data consists
            only of ASCII whitespace characters (space, tab, newline, form feed
            or carriage return).
        """
        return is_empty_text_node(self.node)


@cython.internal
@cython.final
cdef class TextContainer:
    cdef str _text
    cdef str separator
    cdef bint strip

    @staticmethod
    cdef TextContainer new_with_defaults():
        cdef TextContainer cls = TextContainer.__new__(TextContainer)
        cls._text = ''
        cls.separator = ''
        cls.strip = False
        return cls

    def __init__(self, str separator = '', bool strip = False):
        self._text = ""
        self.separator = separator
        self.strip = strip

    def append(self, str node_text):
        if self.strip:
            self._text += node_text.strip() + self.separator
        else:
            self._text += node_text + self.separator

    @property
    def text(self):
        if self.separator and self._text and self._text.endswith(self.separator):
            self._text = self._text[:-len(self.separator)]
        return self._text

cdef lexbor_action_t text_callback(lxb_dom_node_t *node, void *ctx):
    cdef unsigned char *text
    cdef lxb_tag_id_t tag_id = lxb_dom_node_tag_id_noi(node)
    if tag_id != LXB_TAG__TEXT:
        return LEXBOR_ACTION_OK

    text = <unsigned char *> lexbor_str_data_noi(&(<lxb_dom_text_t *> node).char_data.data)
    if not text:
        return LEXBOR_ACTION_OK

    try:
        py_str = text.decode(_ENCODING, "replace")

    except Exception as e:
        PyErr_SetNone(e)
        return LEXBOR_ACTION_STOP

    cdef TextContainer cls
    cls = <TextContainer> ctx
    cls.append(py_str)
    return LEXBOR_ACTION_OK

cdef lxb_status_t serialize_fragment(lxb_dom_node_t *node, lexbor_str_t *lxb_str):
    cdef lxb_status_t status
    while node != NULL:
        status = lxb_html_serialize_tree_str(node, lxb_str)
        if status != LXB_STATUS_OK:
            return status
        node = node.next

    return LXB_STATUS_OK

cdef inline bint _is_node_type(lxb_dom_node_t *node, lxb_dom_node_type_t expected_type):
    return node != NULL and node.type == expected_type
