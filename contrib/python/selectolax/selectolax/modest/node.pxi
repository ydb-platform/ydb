cimport cython
from cpython.exc cimport PyErr_NoMemory

from libc.stdlib cimport free
from libc.stdlib cimport malloc
from libc.stdlib cimport realloc
from libc.string cimport memcpy

DEF _STACK_SIZE = 100
DEF _ENCODING = 'UTF-8'


@cython.final
@cython.internal
cdef class Stack:
    def __cinit__(self, size_t capacity=25):
        self.capacity = capacity
        self.top = 0
        self._stack = <myhtml_tree_node_t**> malloc(capacity * sizeof(myhtml_tree_node_t))
        if self._stack == NULL:
            raise MemoryError("Failed to allocate memory for stack")

    def __dealloc__(self):
        free(self._stack)

    cdef bint is_empty(self):
        return self.top <= 0

    cdef int push(self, myhtml_tree_node_t* res) except -1:
        if self.top >= self.capacity:
            if self.resize() < 0:
                return -1
        self._stack[self.top] = res
        self.top += 1

    cdef myhtml_tree_node_t * pop(self):
        self.top = self.top - 1
        return self._stack[self.top]

    cdef int resize(self) except -1:
        self.capacity *= 2
        self._stack = <myhtml_tree_node_t**> realloc(<void*> self._stack, self.capacity * sizeof(myhtml_tree_node_t))
        if self._stack == NULL:
            PyErr_NoMemory()
            return -1
        return 0

cdef class _Attributes:
    """A dict-like object that represents attributes."""
    cdef myhtml_tree_node_t * node
    cdef unicode decode_errors

    @staticmethod
    cdef _Attributes create(myhtml_tree_node_t *node, unicode decode_errors):
        obj = <_Attributes>_Attributes.__new__(_Attributes)
        obj.node = node
        obj.decode_errors = decode_errors
        return obj

    def __iter__(self):
        cdef myhtml_tree_attr_t *attr = myhtml_node_attribute_first(self.node)
        while attr:
            if attr.key.data == NULL:
                attr = attr.next
                continue
            key = attr.key.data.decode(_ENCODING, self.decode_errors)
            attr = attr.next
            yield key

    def __setitem__(self, str key, value):
        value = str(value)
        bytes_key = key.encode(_ENCODING)
        bytes_value = value.encode(_ENCODING)
        myhtml_attribute_remove_by_key(self.node, <char*>bytes_key, len(bytes_key))
        myhtml_attribute_add(self.node, <char*>bytes_key, len(bytes_key), <char*>bytes_value, len(bytes_value),
                             MyENCODING_UTF_8)

    def __delitem__(self, key):
        try:
            self.__getitem__(key)
        except KeyError:
            raise KeyError(key)
        bytes_key = key.encode(_ENCODING)
        myhtml_attribute_remove_by_key(self.node, <char*>bytes_key, len(bytes_key))

    def __getitem__(self, str key):
        bytes_key = key.encode(_ENCODING)
        cdef myhtml_tree_attr_t * attr =  myhtml_attribute_by_key(self.node, <char*>bytes_key, len(bytes_key))
        if attr != NULL:
            if attr.value.data != NULL:
                return attr.value.data.decode(_ENCODING, self.decode_errors)
            elif attr.key.data != NULL:
                return None
        raise KeyError(key)

    def __len__(self):
        return len(list(self.__iter__()))

    def keys(self):
        return self.__iter__()

    def items(self):
        for key in self.__iter__():
            yield key, self[key]

    def values(self):
        for key in self.__iter__():
            yield self[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def sget(self, key, default=""):
        """Same as get, but returns empty strings instead of None values for empty attributes."""
        try:
            val = self[key]
            if val is None:
                val = ""
            return val
        except KeyError:
            return default

    def __contains__(self, key):
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __repr__(self):
        cdef const char *c_text
        c_text = myhtml_tag_name_by_id(self.node.tree, self.node.tag_id, NULL)
        tag_name = c_text.decode(_ENCODING, 'ignore') if c_text != NULL else 'unknown'
        return "<%s attributes, %s items>" % (tag_name, len(self))

ctypedef fused str_or_Node:
    str
    bytes
    Node

cdef class Node:
    """A class that represents HTML node (element)."""
    cdef myhtml_tree_node_t *node
    cdef public HTMLParser parser

    @staticmethod
    cdef Node new(myhtml_tree_node_t *node, HTMLParser parser):
        # custom __init__ for C, because __cinit__ doesn't accept C types
        cdef Node cls = Node.__new__(Node)
        cls.node = node
        # Keep reference to the selector object, so myhtml structures will not be garbage collected prematurely
        cls.parser = parser
        return cls

    @property
    def attributes(self):
        """Get all attributes that belong to the current node.

        The value of empty attributes is None.

        Returns
        -------
        attributes : dictionary of all attributes.

        Examples
        --------

        >>> tree = HTMLParser("<div data id='my_id'></div>")
        >>> node = tree.css_first('div')
        >>> node.attributes
        {'data': None, 'id': 'my_id'}
        """
        cdef myhtml_tree_attr_t *attr = myhtml_node_attribute_first(self.node)
        attributes = dict()

        while attr:
            if attr.key.data == NULL:
                attr = attr.next
                continue
            key = attr.key.data.decode(_ENCODING, self.parser.decode_errors)
            if attr.value.data:
                value = attr.value.data.decode(_ENCODING, self.parser.decode_errors)
            else:
                value = None
            attributes[key] = value

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

        >>> tree = HTMLParser("<div id='a'></div>")
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
        cdef _Attributes attributes = _Attributes.create(self.node, self.parser.decode_errors)
        return attributes

    @property
    def mem_id(self):
        """Get the mem_id attribute of the node.

        Returns
        -------
        text : int
        """
        return <size_t> self.node

    @property
    def id(self):
        """Get the id attribute of the node.

        Returns None if id does not set.

        Returns
        -------
        text : str
        """
        cdef char* key = 'id'
        cdef myhtml_tree_attr_t *attr
        attr = myhtml_attribute_by_key(self.node, key, 2)
        return None if attr == NULL else attr.value.data.decode(_ENCODING, self.parser.decode_errors)

    def __hash__(self):
        return self.mem_id

    def text(self, bool deep=True, str separator='', bool strip=False):
        """Returns the text of the node including text of all its child nodes.

        Parameters
        ----------
        strip : bool, default False
            If true, calls ``str.strip()`` on each text part to remove extra white spaces.
        separator : str, default ''
            The separator to use when joining text from different nodes.
        deep : bool, default True
            If True, includes text from all child nodes.

        Returns
        -------
        text : str

        """
        text = ""
        cdef const char* c_text
        cdef myhtml_tree_node_t *node = self.node.child

        if not deep:
            if self.node.tag_id == MyHTML_TAG__TEXT:
                c_text = myhtml_node_text(self.node, NULL)
                if c_text != NULL:
                    node_text = c_text.decode(_ENCODING, self.parser.decode_errors)
                    text = append_text(text, node_text, separator, strip)

            while node != NULL:
                if node.tag_id == MyHTML_TAG__TEXT:
                    c_text = myhtml_node_text(node, NULL)
                    if c_text != NULL:
                        node_text = c_text.decode(_ENCODING, self.parser.decode_errors)
                        text = append_text(text, node_text, separator, strip)
                node = node.next
        else:
            text = self._text_deep(self.node, separator=separator, strip=strip)
        if separator and text and text.endswith(separator):
            text = text[:-len(separator)]
        return text

    cdef inline _text_deep(self, myhtml_tree_node_t *node, separator='', strip=False):
        text = ""
        cdef Stack stack = Stack(_STACK_SIZE)
        cdef myhtml_tree_node_t* current_node = NULL

        if node.tag_id == MyHTML_TAG__TEXT:
            c_text = myhtml_node_text(node, NULL)
            if c_text != NULL:
                node_text = c_text.decode(_ENCODING, self.parser.decode_errors)
                text = append_text(text, node_text, separator, strip)

        if node.child == NULL:
            return text

        stack.push(node.child)

        # Depth-first left-to-right tree traversal
        while not stack.is_empty():
            current_node = stack.pop()

            if current_node != NULL:
                if current_node.tag_id == MyHTML_TAG__TEXT:
                    c_text = myhtml_node_text(current_node, NULL)
                    if c_text != NULL:
                        node_text = c_text.decode(_ENCODING, self.parser.decode_errors)
                        text = append_text(text, node_text, separator, strip)

            if current_node.next is not NULL:
                stack.push(current_node.next)

            if current_node.child is not NULL:
                stack.push(current_node.child)

        return text

    def iter(self, include_text=False):
        """Iterate over nodes on the current level.

        Parameters
        ----------
        include_text : bool
            If True, includes text nodes as well.

        Yields
        -------
        node
        """

        cdef myhtml_tree_node_t *node = self.node.child
        cdef Node next_node

        while node != NULL:
            if node.tag_id == MyHTML_TAG__TEXT and not include_text:
                node = node.next
                continue

            next_node = Node.new(node, self.parser)
            yield next_node
            node = node.next

    def traverse(self, include_text=False):
        """Iterate over all child and next nodes starting from the current level.

        Parameters
        ----------
        include_text : bool
            If True, includes text nodes as well.

        Yields
        -------
        node
        """
        cdef Stack stack = Stack(_STACK_SIZE)
        cdef myhtml_tree_node_t* current_node = NULL
        cdef Node next_node

        stack.push(self.node)

        while not stack.is_empty():
            current_node = stack.pop()
            if current_node != NULL and not (current_node.tag_id == MyHTML_TAG__TEXT and not include_text):
                next_node = Node.new(current_node, self.parser)
                yield next_node

            if current_node.next is not NULL:
                stack.push(current_node.next)

            if current_node.child is not NULL:
                stack.push(current_node.child)

    @property
    def tag(self):
        """Return the name of the current tag (e.g. div, p, img).

        Returns
        -------
        text : str
        """
        cdef const char *c_text
        c_text = myhtml_tag_name_by_id(self.node.tree, self.node.tag_id, NULL)
        text = None
        if c_text:
            text = c_text.decode(_ENCODING, self.parser.decode_errors)
        return text

    @property
    def child(self):
        """Alias for the `first_child` property.

        **Deprecated**. Please use `first_child` instead.
        """
        cdef Node node
        if self.node.child:
            node = Node.new(self.node.child, self.parser)
            return node
        return None

    @property
    def parent(self):
        """Return the parent node."""
        cdef Node node
        if self.node.parent:
            node = Node.new(self.node.parent, self.parser)
            return node
        return None

    @property
    def next(self):
        """Return next node."""
        cdef Node node
        if self.node.next:
            node = Node.new(self.node.next, self.parser)
            return node
        return None

    @property
    def prev(self):
        """Return previous node."""
        cdef Node node
        if self.node.prev:
            node = Node.new(self.node.prev, self.parser)
            return node
        return None

    @property
    def last_child(self):
        """Return last child node."""
        cdef Node node
        if self.node.last_child:
            node = Node.new(self.node.last_child, self.parser)
            return node
        return None

    @property
    def html(self):
        """Return HTML representation of the current node including all its child nodes.

        Returns
        -------
        text : str
        """
        cdef mycore_string_raw_t c_str
        c_str.data = NULL
        c_str.length = 0
        c_str.size = 0

        cdef mystatus_t status
        status = myhtml_serialization(self.node, &c_str)

        if status == 0 and c_str.data:
            html = c_str.data.decode(_ENCODING).replace('<-undef>', '')
            free(c_str.data)
            return html

        return None

    def css(self, str query):
        """Evaluate CSS selector against current node and its child nodes."""
        return find_nodes(self.parser, self.node, query)

    def any_css_matches(self, tuple selectors):
        """Returns True if any of CSS selectors matches a node"""
        return find_matches(self.parser, self.node, selectors)

    def css_matches(self, str selector):
        """Returns True if CSS selector matches a node."""
        return find_matches(self.parser, self.node, (selector, ))

    def css_first(self, str query, default=None, bool strict=False):
        """Evaluate CSS selector against current node and its child nodes."""
        results = self.css(query)
        n_results = len(results)

        if n_results > 0:

            if strict and n_results > 1:
                raise ValueError("Expected 1 match, but found %s matches" % n_results)

            return results[0]

        return default

    def decompose(self, bool recursive=True):
        """Remove a Node from the tree.

        Parameters
        ----------
        recursive : bool, default True
            Whenever to delete all its child nodes

        Examples
        --------

        >>> tree = HTMLParser(html)
        >>> for tag in tree.css('script'):
        >>>     tag.decompose()

        """
        if recursive:
            myhtml_node_delete_recursive(self.node)
        else:
            myhtml_node_delete(self.node)

    def remove(self, bool recursive=True):
        """An alias for the decompose method."""
        self.decompose(recursive)

    def unwrap(self, delete_empty = False):
        """Replace node with whatever is inside this node.

        Parameters
        ----------
        delete_empty : bool, default False
            Whenever to delete empty tags.

        Examples
        --------

        >>>  tree = HTMLParser("<div>Hello <i>world</i>!</div>")
        >>>  tree.css_first('i').unwrap()
        >>>  tree.html
        '<html><head></head><body><div>Hello world!</div></body></html>'

        Note: by default, empty tags are ignored, set "delete_empty" to "True" to change this.
        """
        if self.node.child == NULL:
            if delete_empty:
                myhtml_node_delete(self.node)
            return
        cdef myhtml_tree_node_t* next_node
        cdef myhtml_tree_node_t* current_node

        if self.node.child.next != NULL:
            current_node = self.node.child
            next_node = current_node.next

            while next_node != NULL:
                next_node = current_node.next
                myhtml_node_insert_before(self.node, current_node)
                current_node = next_node
        else:
            myhtml_node_insert_before(self.node, self.node.child)
        myhtml_node_delete(self.node)

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

        >>> tree = HTMLParser('<html><head></head><body><script></script><div>Hello world!</div></body></html>')
        >>> tags = ['head', 'style', 'script', 'xmp', 'iframe', 'noembed', 'noframes']
        >>> tree.strip_tags(tags)
        >>> tree.html
        '<html><body><div>Hello world!</div></body></html>'

        """
        # ensure cython can recast element to a Node so that decompose will be called sooner.
        cdef Node element
        for tag in tags:
            for element in self.css(tag):
                element.decompose(recursive=recursive)

    def unwrap_tags(self, list tags, delete_empty = False):
        """Unwraps specified tags from the HTML tree.

        Works the same as the ``unwrap`` method, but applied to a list of tags.

        Parameters
        ----------
        tags : list
            List of tags to remove.
        delete_empty : bool, default False
            Whenever to delete empty tags.

        Examples
        --------

        >>> tree = HTMLParser("<div><a href="">Hello</a> <i>world</i>!</div>")
        >>> tree.body.unwrap_tags(['i','a'])
        >>> tree.body.html
        '<body><div>Hello world!</div></body>'

        Note: by default, empty tags are ignored, set "delete_empty" to "True" to change this.
        """
        cdef Node element
        for tag in tags:
            for element in self.css(tag):
                element.unwrap(delete_empty)

    def replace_with(self, str_or_Node value):
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

        >>> tree = HTMLParser('<div>Get <img src="" alt="Laptop"></div>')
        >>> img = tree.css_first('img')
        >>> img.replace_with(img.attributes.get('alt', ''))
        >>> tree.body.child.html
        '<div>Get Laptop</div>'

        >>> html_parser = HTMLParser('<div>Get <span alt="Laptop"><img src="/jpg"> <div></div></span></div>')
        >>> html_parser2 = HTMLParser('<div>Test</div>')
        >>> img_node = html_parser.css_first('img')
        >>> img_node.replace_with(html_parser2.body.child)
        '<div>Get <span alt="Laptop"><div>Test</div> <div></div></span></div>'
        """
        cdef myhtml_tree_node_t *node
        if isinstance(value, (str, bytes, unicode)):
            bytes_val = to_bytes(value)
            node = myhtml_node_create(self.parser.html_tree, MyHTML_TAG__TEXT, MyHTML_NAMESPACE_HTML)
            myhtml_node_text_set(node, <char*> bytes_val, len(bytes_val), MyENCODING_UTF_8)
            myhtml_node_insert_before(self.node, node)
            myhtml_node_delete(self.node)
        elif isinstance(value, Node):
            node = myhtml_node_clone_deep(self.parser.html_tree, <myhtml_tree_node_t *> value.node)
            myhtml_node_insert_before(self.node, node)
            myhtml_node_delete(self.node)
        else:
            raise TypeError("Expected a string or Node instance, but %s found" % type(value).__name__)

    def insert_before(self, str_or_Node value):
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

        >>> tree = HTMLParser('<div>Get <img src="" alt="Laptop"></div>')
        >>> img = tree.css_first('img')
        >>> img.insert_before(img.attributes.get('alt', ''))
        >>> tree.body.child.html
        '<div>Get Laptop<img src="" alt="Laptop"></div>'

        >>> html_parser = HTMLParser('<div>Get <span alt="Laptop"><img src="/jpg"> <div></div></span></div>')
        >>> html_parser2 = HTMLParser('<div>Test</div>')
        >>> img_node = html_parser.css_first('img')
        >>> img_node.insert_before(html_parser2.body.child)
        <div>Get <span alt="Laptop"><div>Test</div><img src="/jpg"> <div></div></span></div>'
        """
        cdef myhtml_tree_node_t *node
        if isinstance(value, (str, bytes, unicode)):
            bytes_val =  to_bytes(value)
            node = myhtml_node_create(self.parser.html_tree, MyHTML_TAG__TEXT, MyHTML_NAMESPACE_HTML)
            myhtml_node_text_set(node, <char*> bytes_val, len(bytes_val), MyENCODING_UTF_8)
            myhtml_node_insert_before(self.node, node)
        elif isinstance(value, Node):
            node = myhtml_node_clone_deep(self.parser.html_tree, <myhtml_tree_node_t *> value.node)
            myhtml_node_insert_before(self.node, node)
        else:
            raise TypeError("Expected a string or Node instance, but %s found" % type(value).__name__)

    def insert_after(self, str_or_Node value):
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

        >>> tree = HTMLParser('<div>Get <img src="" alt="Laptop"></div>')
        >>> img = tree.css_first('img')
        >>> img.insert_after(img.attributes.get('alt', ''))
        >>> tree.body.child.html
        '<div>Get <img src="" alt="Laptop">Laptop</div>'

        >>> html_parser = HTMLParser('<div>Get <span alt="Laptop"><img src="/jpg"> <div></div></span></div>')
        >>> html_parser2 = HTMLParser('<div>Test</div>')
        >>> img_node = html_parser.css_first('img')
        >>> img_node.insert_after(html_parser2.body.child)
        <div>Get <span alt="Laptop"><img src="/jpg"><div>Test</div> <div></div></span></div>'
        """
        cdef myhtml_tree_node_t *node
        if isinstance(value, (str, bytes, unicode)):
            bytes_val = to_bytes(value)
            node = myhtml_node_create(self.parser.html_tree, MyHTML_TAG__TEXT, MyHTML_NAMESPACE_HTML)
            myhtml_node_text_set(node, <char*> bytes_val, len(bytes_val), MyENCODING_UTF_8)
            myhtml_node_insert_after(self.node, node)
        elif isinstance(value, Node):
            node = myhtml_node_clone_deep(self.parser.html_tree, <myhtml_tree_node_t *> value.node)
            myhtml_node_insert_after(self.node, node)
        else:
            raise TypeError("Expected a string or Node instance, but %s found" % type(value).__name__)

    def insert_child(self, str_or_Node value):
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

        >>> tree = HTMLParser('<div>Get <img src=""></div>')
        >>> div = tree.css_first('div')
        >>> div.insert_child('Laptop')
        >>> tree.body.child.html
        '<div>Get <img src="">Laptop</div>'

        >>> html_parser = HTMLParser('<div>Get <span alt="Laptop"> <div>Laptop</div> </span></div>')
        >>> html_parser2 = HTMLParser('<div>Test</div>')
        >>> span_node = html_parser.css_first('span')
        >>> span_node.insert_child(html_parser2.body.child)
        <div>Get <span alt="Laptop"> <div>Laptop</div> <div>Test</div> </span></div>'
        """
        cdef myhtml_tree_node_t *node
        if isinstance(value, (str, bytes, unicode)):
            bytes_val = to_bytes(value)
            node = myhtml_node_create(self.parser.html_tree, MyHTML_TAG__TEXT, MyHTML_NAMESPACE_HTML)
            myhtml_node_text_set(node, <char*> bytes_val, len(bytes_val), MyENCODING_UTF_8)
            myhtml_node_append_child(self.node, node)
        elif isinstance(value, Node):
            node = myhtml_node_clone_deep(self.parser.html_tree, <myhtml_tree_node_t *> value.node)
            myhtml_node_append_child(self.node, node)
        else:
            raise TypeError("Expected a string or Node instance, but %s found" % type(value).__name__)

    def unwrap_tags(self, list tags, delete_empty = False):
        """Unwraps specified tags from the HTML tree.

        Works the same as th ``unwrap`` method, but applied to a list of tags.

        Parameters
        ----------
        tags : list
            List of tags to remove.
        delete_empty : bool, default False
            Whenever to delete empty tags.

        Examples
        --------

        >>> tree = HTMLParser("<div><a href="">Hello</a> <i>world</i>!</div>")
        >>> tree.body.unwrap_tags(['i','a'])
        >>> tree.body.html
        '<body><div>Hello world!</div></body>'

        Note: by default, empty tags are ignored, set "delete_empty" to "True" to change this.
        """
        cdef Node element
        for tag in tags:
            for element in self.css(tag):
                element.unwrap(delete_empty)

    @property
    def raw_value(self):
        """Return the raw (unparsed, original) value of a node.

        Currently, works on text nodes only.

        Returns
        -------

        raw_value : bytes

        Examples
        --------

        >>> html_parser = HTMLParser('<div>&#x3C;test&#x3E;</div>')
        >>> selector = html_parser.css_first('div')
        >>> selector.child.html
        '&lt;test&gt;'
        >>> selector.child.raw_value
        b'&#x3C;test&#x3E;'
        """
        cdef int begin = self.node.token.element_begin
        cdef int length = self.node.token.element_length
        if self.node.tag_id != MyHTML_TAG__TEXT:
            raise ValueError("Can't obtain raw value for non-text node.")
        return self.parser.raw_html[begin:begin + length]

    def select(self, query=None):
        """Select nodes given a CSS selector.

        Works similarly to the ``css`` method, but supports chained filtering and extra features.

        Parameters
        ----------
        query : str or None
            The CSS selector to use when searching for nodes.

        Returns
        -------
        selector : The `Selector` class.
        """
        return Selector(self, query)

    def scripts_contain(self, str query):
        """Returns True if any of the script tags contain specified text.

        Caches script tags on the first call to improve performance.

        Parameters
        ----------
        query : str
            The query to check.

        """
        cdef Node node
        if self.parser.cached_script_texts is None:
            nodes = find_nodes(self.parser, self.node, 'script')
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
        if self.parser.cached_script_srcs is None:
            nodes = find_nodes(self.parser, self.node, 'script')
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

    def __repr__(self):
        return '<Node %s>' % self.tag

    def __eq__(self, other):
        if isinstance(other, str):
            return self.html == other
        if not isinstance(other, Node):
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
        text = ""
        cdef const char* c_text
        cdef myhtml_tree_node_t *node = self.node.child

        if self.node.tag_id == MyHTML_TAG__TEXT:
            c_text = myhtml_node_text(self.node, NULL)
            if c_text != NULL:
                node_text = c_text.decode(_ENCODING, self.parser.decode_errors)
                return append_text(text, node_text)
        return None

    def merge_text_nodes(self):
        """Iterates over all text nodes and merges all text nodes that are close to each other.

        This is useful for text extraction.
        Use it when you need to strip HTML tags and merge "dangling" text.

        Examples
        --------

        >>> tree = HTMLParser("<div><p><strong>J</strong>ohn</p><p>Doe</p></div>")
        >>> node = tree.css_first('div')
        >>> tree.unwrap_tags(["strong"])
        >>> tree.text(deep=True, separator=" ", strip=True)
        "J ohn Doe" # Text extraction produces an extra space because the strong tag was removed.
        >>> node.merge_text_nodes()
        >>> tree.text(deep=True, separator=" ", strip=True)
        "John Doe"
        """
        cdef Stack stack = Stack(_STACK_SIZE)
        cdef myhtml_tree_node_t * current_node = NULL
        cdef Node next_node
        cdef const char* left_text
        cdef const char* right_text
        cdef char* final_text
        cdef size_t left_length, right_length, final_length

        stack.push(self.node)

        while not stack.is_empty():
            current_node = stack.pop()

            if (current_node.tag_id == MyHTML_TAG__TEXT and current_node.prev and
                    current_node.prev.tag_id == MyHTML_TAG__TEXT):
                left_text = myhtml_node_text(current_node.prev, &left_length)
                right_text = myhtml_node_text(current_node, &right_length)
                if left_text and right_text:
                    final_length = left_length + right_length
                    final_text = <char *>malloc(final_length + 1)
                    if final_text == NULL:
                        raise MemoryError("Can't allocate memory for a new node.")
                    memcpy(final_text, left_text, left_length)
                    memcpy(final_text + left_length, right_text, right_length + 1)
                    myhtml_node_text_set(current_node, <const char *>final_text, final_length, MyENCODING_UTF_8)
                    myhtml_node_delete(current_node.prev)
                    free(final_text)

            if current_node.next is not NULL:
                stack.push(current_node.next)

            if current_node.child is not NULL:
                stack.push(current_node.child)

cdef inline str append_text(str text, str node_text, str separator='', bint strip=False):
    if strip:
        text += node_text.strip() + separator
    else:
        text += node_text + separator

    return text


cdef inline bytes to_bytes(str_or_Node value):
    cdef bytes bytes_val
    if isinstance(value, unicode):
        bytes_val = <bytes>value.encode("utf-8")
    elif isinstance(value, bytes):
        bytes_val = <bytes>value
    return bytes_val
