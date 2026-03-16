cimport cython
from cpython.exc cimport PyErr_SetObject


@cython.final
cdef class CSSSelector:

    cdef char *c_selector
    cdef mycss_entry_t *css_entry
    cdef modest_finder_t *finder
    cdef mycss_selectors_list_t *selectors_list

    def __init__(self, str selector):

        selector_pybyte = selector.encode('UTF-8')
        self.c_selector = selector_pybyte

        # In order to propagate errors these methods should return no value
        self._create_css_parser()
        self._prepare_selector(self.css_entry, self.c_selector, len(self.c_selector))
        self.finder = modest_finder_create_simple()

    cdef myhtml_collection_t* find(self, myhtml_tree_node_t* scope):
        """Find all possible matches."""

        cdef myhtml_collection_t *collection

        collection = NULL
        modest_finder_by_selectors_list(self.finder, scope, self.selectors_list, &collection)

        return collection

    cdef int _create_css_parser(self) except -1:
        cdef mystatus_t status

        cdef mycss_t *mycss = mycss_create()
        status = mycss_init(mycss)

        if status != 0:
            PyErr_SetObject(RuntimeError, "Can't init MyCSS object.")
            return -1

        self.css_entry = mycss_entry_create()
        status = mycss_entry_init(mycss, self.css_entry)

        if status != 0:
            PyErr_SetObject(RuntimeError, "Can't init MyCSS Entry object.")
            return -1
        return 0

    cdef int _prepare_selector(self, mycss_entry_t *css_entry, const char *selector, size_t selector_size) except -1:
        cdef mystatus_t out_status
        self.selectors_list = mycss_selectors_parse(mycss_entry_selectors(css_entry), myencoding_t.MyENCODING_UTF_8,
                                                    selector, selector_size, &out_status)

        if (self.selectors_list == NULL) or (self.selectors_list.flags and MyCSS_SELECTORS_FLAGS_SELECTOR_BAD):
            PyErr_SetObject(ValueError, "Bad CSS Selectors: %s" % self.c_selector.decode('utf-8'))
            return -1
        return 0

    def __dealloc__(self):
        mycss_selectors_list_destroy(mycss_entry_selectors(self.css_entry), self.selectors_list, 1)
        modest_finder_destroy(self.finder, 1)

        cdef mycss_t *mycss = self.css_entry.mycss
        mycss_entry_destroy(self.css_entry, 1)
        mycss_destroy(mycss, 1)


cdef class Selector:
    """An advanced CSS selector that supports additional operations.

    Think of it as a toolkit that mimics some of the features of XPath.

    Please note, this is an experimental feature that can change in the future.
    """
    cdef Node node
    cdef list nodes

    def __init__(self, Node node, str query):
        """custom init, because __cinit__ doesn't accept C types"""
        self.node = node
        self.nodes = find_nodes(node.parser, node.node, query) if query else [node, ]

    cpdef css(self, str query):
        """Evaluate CSS selector against current scope."""
        cdef Node current_node
        nodes = list()
        for node in self.nodes:
            current_node = node
            nodes.extend(find_nodes(self.node.parser, current_node.node, query))
        self.nodes = nodes
        return self

    @property
    def matches(self):
        """Returns all possible matches"""
        return self.nodes

    @property
    def any_matches(self):
        """Returns True if there are any matches"""
        return bool(self.nodes)

    def text_contains(self, str text, bool deep=True, str separator='', bool strip=False):
        """Filter all current matches given text."""
        nodes = []
        cdef Node node
        for node in self.nodes:
            node_text = node.text(deep=deep, separator=separator, strip=strip)
            if node_text and text in node_text:
                nodes.append(node)
        self.nodes = nodes
        return self

    def any_text_contains(self, str text, bool deep=True, str separator='', bool strip=False):
        """Returns True if any node in the current search scope contains specified text"""
        nodes = []
        cdef Node node
        for node in self.nodes:
            node_text = node.text(deep=deep, separator=separator, strip=strip)
            if node_text and text in node_text:
                return True
        return False

    def attribute_longer_than(self, str attribute, int length, str start  = None):
        """Filter all current matches by attribute length.

        Similar to `string-length` in XPath.
        """
        nodes = []
        for node in self.nodes:
            attr = node.attributes.get(attribute)
            if attr and start and start in attr:
                attr = attr[attr.find(start) + len(start):]
            if len(attr) > length:
                nodes.append(node)
        self.nodes = nodes
        return self

    def any_attribute_longer_than(self, str attribute, int length, str start  = None):
        """Returns True any href attribute longer than a specified length.

        Similar to `string-length` in XPath.
        """
        cdef list nodes = []
        cdef Node node
        for node in self.nodes:
            attr = node.attributes.get(attribute)
            if attr and start and start in attr:
                attr = attr[attr.find(start) + len(start):]
            if len(attr) > length:
                return True
        return False

    def __bool__(self):
        return bool(self.nodes)

cdef find_nodes(HTMLParser parser, myhtml_tree_node_t *node, str query):
    cdef myhtml_collection_t *collection
    cdef CSSSelector selector = CSSSelector(query)
    cdef Node n
    cdef list result = []
    collection = selector.find(node)

    if collection == NULL:
        return result

    for i in range(collection.length):
        n = Node.new(collection.list[i], parser)
        result.append(n)
    myhtml_collection_destroy(collection)
    return result


cdef bool find_matches(HTMLParser parser, myhtml_tree_node_t *node, tuple selectors):
    cdef myhtml_collection_t *collection
    cdef CSSSelector selector
    cdef int collection_size
    cdef str query

    for query in selectors:
        selector = CSSSelector(query)
        collection_size = 0
        collection = NULL

        collection = selector.find(node)
        if collection == NULL:
            continue

        collection_size = collection.length
        myhtml_collection_destroy(collection)
        if collection_size > 0:
            return True
    return False
