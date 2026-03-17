# cython: language_level=2

cimport cython

cdef tuple _getNsTag(tag)

cdef class ElementTreeProducer:
    cdef _element
    cdef _content_handler
    cdef _attr_class
    cdef _empty_attributes

    @cython.locals(element_nsmap=dict)
    cdef inline _recursive_saxify(self, element, dict parent_nsmap)

    cdef inline _build_qname(self, ns_uri, local_name, dict nsmap, preferred_prefix, bint is_attribute)
