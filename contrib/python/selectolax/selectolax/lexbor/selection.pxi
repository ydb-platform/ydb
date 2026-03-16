cimport cython
from cpython.exc cimport PyErr_SetObject
from cpython.list cimport PyList_GET_SIZE


@cython.final
cdef class LexborCSSSelector:

    def __init__(self):
        self._create_css_parser()
        self.results = []
        self.current_node = None

    cdef int _create_css_parser(self) except -1:
        cdef lxb_status_t status

        self.parser = lxb_css_parser_create()
        status = lxb_css_parser_init(self.parser, NULL)

        if status != LXB_STATUS_OK:
            PyErr_SetObject(SelectolaxError, "Can't initialize CSS parser.")
            return -1

        self.css_selectors = lxb_css_selectors_create()
        status = lxb_css_selectors_init(self.css_selectors)

        if status != LXB_STATUS_OK:
            PyErr_SetObject(SelectolaxError, "Can't initialize CSS selector.")
            return -1

        lxb_css_parser_selectors_set(self.parser, self.css_selectors)

        self.selectors = lxb_selectors_create()
        status = lxb_selectors_init(self.selectors)
        lxb_selectors_opt_set(self.selectors, LXB_SELECTORS_OPT_MATCH_ROOT)
        if status != LXB_STATUS_OK:
            PyErr_SetObject(SelectolaxError, "Can't initialize CSS selector.")
            return -1
        return 0

    cpdef list find(self, str query, LexborNode node):
        return self._find(query, node, 0)

    cpdef list find_first(self, str query, LexborNode node):
        return self._find(query, node, 1)

    cpdef list _find(self, str query, LexborNode node, bint only_first):
        cdef lxb_css_selector_list_t* selectors
        cdef lxb_char_t* c_selector
        cdef lxb_css_selector_list_t * selectors_list

        if not isinstance(query, str):
            raise TypeError("Query must be a string.")

        bytes_query = query.encode(_ENCODING)
        selectors_list = lxb_css_selectors_parse(self.parser, <lxb_char_t *> bytes_query, <size_t>len(bytes_query))

        if selectors_list == NULL:
            raise SelectolaxError("Can't parse CSS selector.")

        self.current_node = node
        self.results = []
        if only_first:
            status = lxb_selectors_find(self.selectors, node.node, selectors_list,
                                        <lxb_selectors_cb_f>css_finder_callback_first, <void*>self)
        else:
            status = lxb_selectors_find(self.selectors, node.node, selectors_list,
                                        <lxb_selectors_cb_f>css_finder_callback, <void*>self)
        results = list(self.results)
        self.results = []
        self.current_node = None
        lxb_css_selector_list_destroy_memory(selectors_list)
        return results

    cpdef int any_matches(self, str query, LexborNode node) except -1:
        cdef lxb_css_selector_list_t * selectors
        cdef lxb_char_t * c_selector
        cdef lxb_css_selector_list_t * selectors_list
        cdef int result

        if not isinstance(query, str):
            raise TypeError("Query must be a string.")

        bytes_query = query.encode(_ENCODING)
        selectors_list = lxb_css_selectors_parse(self.parser, <lxb_char_t *> bytes_query, <size_t> len(query))

        if selectors_list == NULL:
            PyErr_SetObject(SelectolaxError, "Can't parse CSS selector.")
            return -1

        self.results = []
        status = lxb_selectors_find(self.selectors, node.node, selectors_list,
                                    <lxb_selectors_cb_f> css_matcher_callback, <void *> self)
        if status != LXB_STATUS_OK:
            lxb_css_selector_list_destroy_memory(selectors_list)
            PyErr_SetObject(SelectolaxError, "Can't parse CSS selector.")
            return -1

        result = PyList_GET_SIZE(self.results) > 0
        self.results = []
        lxb_css_selector_list_destroy_memory(selectors_list)
        return result

    def __dealloc__(self):
        if self.selectors != NULL:
            lxb_selectors_destroy(self.selectors, True)
        if self.parser != NULL:
            lxb_css_parser_destroy(self.parser, True)
        if self.css_selectors != NULL:
            lxb_css_selectors_destroy(self.css_selectors, True)


cdef class LexborSelector:
    """An advanced CSS selector that supports additional operations.

    Think of it as a toolkit that mimics some of the features of XPath.

    Please note, this is an experimental feature that can change in the future.
    """
    cdef LexborNode node
    cdef list nodes

    def __init__(self, LexborNode node, query):
        self.node = node
        self.nodes = self.node.parser.selector.find(query, self.node) if query else [node, ]

    cpdef css(self, str query):
        """Evaluate CSS selector against current scope."""
        raise NotImplementedError("This features is not supported by the lexbor backend. Please use Modest backend.")

    @property
    def matches(self) -> list:
        """Returns all possible matches"""
        return self.nodes

    @property
    def any_matches(self) -> bool:
        """Returns True if there are any matches"""
        return bool(self.nodes)

    def text_contains(self, str text, bool deep=True, str separator='', bool strip=False) -> LexborSelector:
        """Filter all current matches given text."""
        cdef list nodes = []
        for node in self.nodes:
            node_text = node.text(deep=deep, separator=separator, strip=strip)
            if node_text and text in node_text:
                nodes.append(node)
        self.nodes = nodes
        return self

    def any_text_contains(self, str text, bool deep=True, str separator='', bool strip=False) -> bool:
        """Returns True if any node in the current search scope contains specified text"""
        cdef LexborNode node
        for node in self.nodes:
            node_text = node.text(deep=deep, separator=separator, strip=strip)
            if node_text and text in node_text:
                return True
        return False

    def attribute_longer_than(self, str attribute, int length, str start  = None) -> LexborSelector:
        """Filter all current matches by attribute length.

        Similar to `string-length` in XPath.
        """
        cdef list nodes = []
        for node in self.nodes:
            attr = node.attributes.get(attribute)
            if not attr:
                continue
            if attr and start and start in attr:
                attr = attr[attr.find(start) + len(start):]
            if len(attr) > length:
                nodes.append(node)
        self.nodes = nodes
        return self

    def any_attribute_longer_than(self, str attribute, int length, str start  = None) -> bool:
        """Returns True any href attribute longer than a specified length.

        Similar to `string-length` in XPath.
        """
        cdef LexborNode node
        for node in self.nodes:
            attr = node.attributes.get(attribute)
            if attr and start and start in attr:
                attr = attr[attr.find(start) + len(start):]
            if len(attr) > length:
                return True
        return False

    def __bool__(self):
        return bool(self.nodes)


cdef lxb_status_t css_finder_callback(lxb_dom_node_t *node, lxb_css_selector_specificity_t *spec, void *ctx):
    cdef LexborNode lxb_node
    cdef LexborCSSSelector cls
    cls = <LexborCSSSelector> ctx
    lxb_node = LexborNode.new(<lxb_dom_node_t *> node, cls.current_node.parser)
    cls.results.append(lxb_node)
    return LXB_STATUS_OK

cdef lxb_status_t css_finder_callback_first(lxb_dom_node_t *node, lxb_css_selector_specificity_t *spec, void *ctx):
    cdef LexborNode lxb_node
    cdef LexborCSSSelector cls
    cls = <LexborCSSSelector> ctx
    lxb_node = LexborNode.new(<lxb_dom_node_t *> node, cls.current_node.parser)
    cls.results.append(lxb_node)
    return LXB_STATUS_STOP


cdef lxb_status_t css_matcher_callback(lxb_dom_node_t *node, lxb_css_selector_specificity_t *spec, void *ctx):
    cdef LexborNode lxb_node
    cdef LexborCSSSelector cls
    cls = <LexborCSSSelector> ctx
    cls.results.append(True)
    return LXB_STATUS_STOP
