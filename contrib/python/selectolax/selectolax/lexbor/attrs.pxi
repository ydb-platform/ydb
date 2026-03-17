cimport cython


@cython.final
cdef class LexborAttributes:
    """A dict-like object that represents attributes."""
    cdef lxb_dom_node_t *node
    cdef unicode decode_errors

    @staticmethod
    cdef LexborAttributes create(lxb_dom_node_t *node):
        obj = <LexborAttributes> LexborAttributes.__new__(LexborAttributes)
        obj.node = node
        return obj

    def __iter__(self):
        cdef lxb_dom_attr_t *attr = lxb_dom_element_first_attribute_noi(<lxb_dom_element_t *> self.node)
        cdef size_t str_len = 0
        attributes = dict()

        while attr != NULL:
            key = lxb_dom_attr_local_name_noi(attr, &str_len)
            if key is not NULL:
                yield key.decode(_ENCODING)
            attr = attr.next

    def __setitem__(self, str key, object value):
        value = value
        bytes_key = key.encode(_ENCODING)
        bytes_value = value.encode(_ENCODING) if value else b""
        cdef lxb_dom_attr_t *attr
        cdef lxb_dom_document_t *doc

        if value is None:
            # N.B. This is suboptimal, but there is not API to set empty attributes
            attr = lxb_dom_element_set_attribute(
                <lxb_dom_element_t *> self.node,
                <lxb_char_t *> bytes_key, len(bytes_key),
                NULL, 0
            )
            doc = (<lxb_dom_node_t*>attr).owner_document
            lexbor_str_destroy(attr.value, doc.text, 0)
            attr.value = NULL

        elif isinstance(value, str) or isinstance(value, unicode) :
            lxb_dom_element_set_attribute(
                <lxb_dom_element_t *> self.node,
                <lxb_char_t *> bytes_key, len(bytes_key),
                <lxb_char_t *> bytes_value, len(bytes_value),
            )
        else:
            raise TypeError("Expected str or unicode, got %s" % type(value))

    def __delitem__(self, key):
        try:
            self.__getitem__(key)
        except KeyError:
            raise KeyError(key)
        bytes_key = key.encode(_ENCODING)
        lxb_dom_element_remove_attribute(
            <lxb_dom_element_t *> self.node,
            <lxb_char_t *> bytes_key, len(bytes_key),
        )

    def __getitem__(self, str key):
        bytes_key = key.encode(_ENCODING)
        cdef lxb_dom_attr_t * attr = lxb_dom_element_attr_by_name(
            <lxb_dom_element_t *> self.node,
            <lxb_char_t *> bytes_key, len(bytes_key)
        )
        cdef size_t str_len = 0
        if attr != NULL:
            value = lxb_dom_attr_value_noi(attr, &str_len)
            return value.decode(_ENCODING) if value else None
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
        cdef lxb_char_t *c_text
        cdef size_t str_len = 0
        c_text = lxb_dom_element_qualified_name(<lxb_dom_element_t *> self.node, &str_len)
        tag_name = c_text.decode(_ENCODING, 'ignore') if c_text != NULL else 'unknown'
        return "<%s attributes, %s items>" % (tag_name, len(self))
