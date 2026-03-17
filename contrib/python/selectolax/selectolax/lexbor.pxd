from libc.stdint cimport uint8_t, uint32_t, uintptr_t


cdef extern from "lexbor/core/core.h" nogil:
    ctypedef uint32_t lxb_codepoint_t
    ctypedef unsigned char lxb_char_t
    ctypedef unsigned int lxb_status_t
    ctypedef enum lexbor_status_t:
        LXB_STATUS_OK = 0x0000
        LXB_STATUS_ERROR = 0x0001
        LXB_STATUS_ERROR_MEMORY_ALLOCATION
        LXB_STATUS_ERROR_OBJECT_IS_NULL
        LXB_STATUS_ERROR_SMALL_BUFFER
        LXB_STATUS_ERROR_INCOMPLETE_OBJECT
        LXB_STATUS_ERROR_NO_FREE_SLOT
        LXB_STATUS_ERROR_TOO_SMALL_SIZE
        LXB_STATUS_ERROR_NOT_EXISTS
        LXB_STATUS_ERROR_WRONG_ARGS
        LXB_STATUS_ERROR_WRONG_STAGE
        LXB_STATUS_ERROR_UNEXPECTED_RESULT
        LXB_STATUS_ERROR_UNEXPECTED_DATA
        LXB_STATUS_ERROR_OVERFLOW
        LXB_STATUS_CONTINUE
        LXB_STATUS_SMALL_BUFFER
        LXB_STATUS_ABORTED
        LXB_STATUS_STOPPED
        LXB_STATUS_NEXT
        LXB_STATUS_STOP

    lexbor_str_t* lexbor_str_destroy(lexbor_str_t *str, lexbor_mraw_t *mraw, bint destroy_obj)

    lexbor_str_t* lexbor_str_create()
    lxb_char_t * lexbor_str_data_noi(lexbor_str_t *str)

cdef extern from "lexbor/html/html.h" nogil:
    ctypedef unsigned int lxb_html_document_opt_t

    ctypedef struct lxb_html_tokenizer_t
    ctypedef struct lxb_html_form_element_t
    ctypedef struct lxb_html_head_element_t
    ctypedef struct lxb_html_body_element_t
    ctypedef struct lxb_dom_element_t
    ctypedef struct lexbor_mraw_t
    ctypedef struct lexbor_hash_t
    ctypedef struct lxb_dom_document_type_t
    ctypedef void lxb_dom_interface_t
    ctypedef uintptr_t lxb_tag_id_t
    ctypedef uintptr_t lxb_ns_id_t
    ctypedef lxb_dom_interface_t *(*lxb_dom_interface_destroy_f)(lxb_dom_interface_t *intrfc)
    ctypedef lxb_dom_interface_t *(*lxb_dom_interface_create_f)(lxb_dom_document_t *document, lxb_tag_id_t tag_id,
                                                                lxb_ns_id_t ns)

    ctypedef struct lxb_dom_event_target_t:
        void *events

    ctypedef struct lexbor_str_t:
        lxb_char_t *data
        size_t     length

    ctypedef struct lxb_dom_node_t:
        lxb_dom_event_target_t event_target

        uintptr_t              local_name
        uintptr_t              prefix
        uintptr_t              ns

        lxb_dom_document_t     *owner_document

        lxb_dom_node_t         *next
        lxb_dom_node_t         *prev
        lxb_dom_node_t         *parent
        lxb_dom_node_t         *first_child
        lxb_dom_node_t         *last_child
        void                   *user

        lxb_dom_node_type_t    type

    ctypedef struct lxb_dom_document_t:
        lxb_dom_node_t              node

        lxb_dom_document_cmode_t    compat_mode
        lxb_dom_document_dtype_t    type

        lxb_dom_document_type_t *doctype
        lxb_dom_element_t *element

        lxb_dom_interface_create_f  create_interface
        lxb_dom_interface_destroy_f destroy_interface

        lexbor_mraw_t *mraw
        lexbor_mraw_t *text
        lexbor_hash_t *tags
        lexbor_hash_t *attrs
        lexbor_hash_t *prefix
        lexbor_hash_t *ns
        void *parser
        void *user

        bint                        tags_inherited
        bint                        ns_inherited

        bint                        scripting

    ctypedef  struct lxb_html_document_t:
        lxb_dom_document_t dom_document

        void *iframe_srcdoc

        lxb_html_head_element_t *head
        lxb_html_body_element_t *body
        lxb_html_document_ready_state_t ready_state
        lxb_html_document_opt_t         opt

    ctypedef  enum lxb_html_document_ready_state_t:
        LXB_HTML_DOCUMENT_READY_STATE_UNDEF       = 0x00
        LXB_HTML_DOCUMENT_READY_STATE_LOADING     = 0x01
        LXB_HTML_DOCUMENT_READY_STATE_INTERACTIVE = 0x02
        LXB_HTML_DOCUMENT_READY_STATE_COMPLETE    = 0x03

    ctypedef enum lxb_html_parser_state_t:
        LXB_HTML_PARSER_STATE_BEGIN            = 0x00
        LXB_HTML_PARSER_STATE_PROCESS          = 0x01
        LXB_HTML_PARSER_STATE_END              = 0x02
        LXB_HTML_PARSER_STATE_FRAGMENT_PROCESS = 0x03
        LXB_HTML_PARSER_STATE_ERROR            = 0x04

    ctypedef enum lxb_dom_node_type_t:
        LXB_DOM_NODE_TYPE_ELEMENT                = 0x01
        LXB_DOM_NODE_TYPE_ATTRIBUTE              = 0x02
        LXB_DOM_NODE_TYPE_TEXT                   = 0x03
        LXB_DOM_NODE_TYPE_CDATA_SECTION          = 0x04
        LXB_DOM_NODE_TYPE_ENTITY_REFERENCE       = 0x05
        LXB_DOM_NODE_TYPE_ENTITY                 = 0x06
        LXB_DOM_NODE_TYPE_PROCESSING_INSTRUCTION = 0x07
        LXB_DOM_NODE_TYPE_COMMENT                = 0x08
        LXB_DOM_NODE_TYPE_DOCUMENT               = 0x09
        LXB_DOM_NODE_TYPE_DOCUMENT_TYPE          = 0x0A
        LXB_DOM_NODE_TYPE_DOCUMENT_FRAGMENT      = 0x0B
        LXB_DOM_NODE_TYPE_NOTATION               = 0x0C
        LXB_DOM_NODE_TYPE_LAST_ENTRY             = 0x0D

    ctypedef enum lxb_dom_document_cmode_t:
        LXB_DOM_DOCUMENT_CMODE_NO_QUIRKS = 0x00
        LXB_DOM_DOCUMENT_CMODE_QUIRKS = 0x01
        LXB_DOM_DOCUMENT_CMODE_LIMITED_QUIRKS = 0x02

    ctypedef enum lxb_dom_document_dtype_t:
        LXB_DOM_DOCUMENT_DTYPE_UNDEF = 0x00,
        LXB_DOM_DOCUMENT_DTYPE_HTML = 0x01,
        LXB_DOM_DOCUMENT_DTYPE_XML = 0x02

    ctypedef enum lxb_html_serialize_opt_t:
        LXB_HTML_SERIALIZE_OPT_UNDEF = 0x00
        LXB_HTML_SERIALIZE_OPT_SKIP_WS_NODES = 0x01
        LXB_HTML_SERIALIZE_OPT_SKIP_COMMENT = 0x02
        LXB_HTML_SERIALIZE_OPT_RAW = 0x04
        LXB_HTML_SERIALIZE_OPT_WITHOUT_CLOSING = 0x08
        LXB_HTML_SERIALIZE_OPT_TAG_WITH_NS = 0x10
        LXB_HTML_SERIALIZE_OPT_WITHOUT_TEXT_INDENT = 0x20
        LXB_HTML_SERIALIZE_OPT_FULL_DOCTYPE = 0x40

    ctypedef struct lexbor_array_t:
        void **list
        size_t size
        size_t length

    ctypedef struct lexbor_array_obj_t:
        uint8_t *list
        size_t  size
        size_t  length
        size_t  struct_size

    ctypedef struct lxb_html_tree_pending_table_t
    ctypedef bint lxb_html_tree_insertion_mode_f
    ctypedef lxb_status_t lxb_html_tree_append_attr_f

    ctypedef struct lxb_html_tree_t:

        lxb_html_tokenizer_t *tkz_ref

        lxb_html_document_t *document
        lxb_dom_node_t *fragment

        lxb_html_form_element_t *form

        lexbor_array_t *open_elements
        lexbor_array_t *active_formatting
        lexbor_array_obj_t *template_insertion_modes

        lxb_html_tree_pending_table_t *pending_table

        lexbor_array_obj_t *parse_errors

        bint foster_parenting
        bint frameset_ok
        bint scripting

        lxb_html_tree_insertion_mode_f mode
        lxb_html_tree_insertion_mode_f original_mode
        lxb_html_tree_append_attr_f    before_append_attr

        lxb_status_t status

        size_t ref_count

    ctypedef struct lxb_html_parser_t:
        lxb_html_tokenizer_t *tkz
        lxb_html_tree_t *tree
        lxb_html_tree_t *original_tree

        lxb_dom_node_t *root
        lxb_dom_node_t *form

        lxb_html_parser_state_t state
        lxb_status_t status

        size_t  ref_count

    ctypedef struct lxb_html_element_t

    # Functions
    lxb_html_document_t * lxb_html_document_create()
    lxb_html_element_t * lxb_html_document_create_element(lxb_html_document_t *document,
                                                          const lxb_char_t *local_name, size_t lname_len,
                                                          void *reserved_for_opt)
    lxb_status_t lxb_html_document_parse(lxb_html_document_t *document, const lxb_char_t *html, size_t size)
    lxb_dom_node_t * lxb_html_document_parse_fragment(lxb_html_document_t *document,
                                                      lxb_dom_element_t *element,
                                                      const lxb_char_t *html,
                                                      size_t size)
    lxb_html_body_element_t * lxb_html_document_body_element_noi(lxb_html_document_t *document)
    lxb_html_head_element_t * lxb_html_document_head_element_noi(lxb_html_document_t *document)
    lxb_dom_element_t * lxb_dom_document_element(lxb_dom_document_t *document)

    lxb_status_t lxb_html_serialize_tree_str(lxb_dom_node_t *node, lexbor_str_t *str)
    lxb_status_t lxb_html_serialize_deep_str(lxb_dom_node_t *node, lexbor_str_t *str)
    lxb_html_element_t* lxb_html_element_inner_html_set(lxb_html_element_t *element,
                                                        const lxb_char_t *html, size_t size)

cdef class LexborNode:
    cdef:
        lxb_dom_node_t *node
        public LexborHTMLParser parser
        cdef bint _is_fragment_root

    @staticmethod
    cdef LexborNode new(lxb_dom_node_t *node, LexborHTMLParser parser)
    cdef void set_as_fragment_root(self)
    cdef inline LexborNode _get_node(self)


cdef bint is_empty_text_node(lxb_dom_node_t *node)
cdef inline bint _is_whitespace_only(const lxb_char_t *buffer, size_t buffer_length) nogil


cdef class LexborCSSSelector:
    cdef lxb_css_parser_t* parser
    cdef lxb_selectors_t * selectors
    cdef lxb_css_selectors_t * css_selectors
    cdef public list results
    cdef public LexborNode current_node
    cdef int _create_css_parser(self) except -1
    cpdef list find(self, str query, LexborNode node)
    cpdef list find_first(self, str query, LexborNode node)
    cpdef list _find(self, str query, LexborNode node, bint only_first)
    cpdef int any_matches(self, str query, LexborNode node) except -1

cdef class LexborHTMLParser:
    cdef lxb_html_document_t *document
    cdef lxb_html_document_t *_fragment_document
    cdef bint _is_fragment
    cdef public bytes raw_html
    cdef LexborCSSSelector _selector
    cdef inline void _new_html_document(self)
    cdef inline lxb_status_t _parse_html_document(self, char *html, size_t html_len) nogil
    cdef inline lxb_status_t _parse_html_fragment(self, char *html, size_t html_len) nogil
    cdef int _parse_html(self, char *html, size_t html_len) except -1
    cdef object cached_script_texts
    cdef object cached_script_srcs

    @staticmethod
    cdef LexborHTMLParser from_document(lxb_html_document_t * document, bytes raw_html)
    cdef inline lxb_html_document_t* main_document(self) nogil

cdef extern from "lexbor/dom/dom.h" nogil:
    ctypedef enum lexbor_action_t:
        LEXBOR_ACTION_OK    = 0x00
        LEXBOR_ACTION_STOP  = 0x01
        LEXBOR_ACTION_NEXT  = 0x02

    ctypedef lexbor_action_t (*lxb_dom_node_simple_walker_f)(lxb_dom_node_t *node, void *ctx)

    ctypedef struct lxb_dom_character_data_t:
        lxb_dom_node_t node
        lexbor_str_t   data

    ctypedef struct lxb_dom_text_t:
        lxb_dom_character_data_t char_data

    ctypedef uintptr_t lxb_dom_attr_id_t
    ctypedef struct lxb_dom_collection_t:
        lexbor_array_t     array
        lxb_dom_document_t *document

    ctypedef struct lxb_dom_attr_t:
        lxb_dom_node_t     node

        lxb_dom_attr_id_t  upper_name
        lxb_dom_attr_id_t  qualified_name

        lexbor_str_t *value

        lxb_dom_element_t *owner

        lxb_dom_attr_t *next
        lxb_dom_attr_t *prev

    lxb_dom_collection_t * lxb_dom_collection_make(lxb_dom_document_t *document, size_t start_list_size)
    lxb_char_t * lxb_dom_node_text_content(lxb_dom_node_t *node, size_t *len)
    lxb_status_t lxb_dom_node_text_content_set(lxb_dom_node_t *node, const lxb_char_t *content, size_t len)
    bint lxb_dom_node_is_empty(lxb_dom_node_t *node)
    void lxb_dom_node_remove(lxb_dom_node_t *node)
    void * lxb_dom_document_destroy_text_noi(lxb_dom_document_t *document, lxb_char_t *text)
    lxb_dom_node_t * lxb_dom_document_root(lxb_dom_document_t *document)
    lxb_dom_element_t * lxb_dom_interface_element(lxb_dom_node_t *node)
    lxb_char_t * lxb_dom_element_qualified_name(lxb_dom_element_t *element, size_t *len)
    lxb_dom_node_t * lxb_dom_node_destroy(lxb_dom_node_t *node)
    lxb_dom_node_t * lxb_dom_node_destroy_deep(lxb_dom_node_t *root)
    lxb_dom_attr_t * lxb_dom_element_first_attribute_noi(lxb_dom_element_t *element)

    const lxb_char_t * lxb_dom_attr_local_name_noi(lxb_dom_attr_t *attr, size_t *len)
    const lxb_char_t * lxb_dom_attr_value_noi(lxb_dom_attr_t *attr, size_t *len)

    lxb_dom_attr_t * lxb_dom_element_set_attribute(lxb_dom_element_t *element,
                                                   const lxb_char_t *qualified_name, size_t qn_len,
                                                   const lxb_char_t *value, size_t value_len)
    lxb_status_t lxb_dom_element_remove_attribute(lxb_dom_element_t *element,
                                                  const lxb_char_t *qualified_name, size_t qn_len)
    lxb_dom_attr_t * lxb_dom_element_attr_by_name(lxb_dom_element_t *element,
                                                  const lxb_char_t *qualified_name, size_t length)
    lxb_tag_id_t lxb_dom_node_tag_id_noi(lxb_dom_node_t *node)
    lxb_dom_node_t * lxb_dom_document_import_node(lxb_dom_document_t *doc, lxb_dom_node_t *node, bint deep)
    void lxb_dom_node_insert_after(lxb_dom_node_t *to, lxb_dom_node_t *node)
    lxb_status_t lxb_dom_node_replace_all(lxb_dom_node_t *parent, lxb_dom_node_t *node)
    void lxb_dom_node_insert_child(lxb_dom_node_t *to, lxb_dom_node_t *node)
    void lxb_dom_node_insert_before(lxb_dom_node_t *to, lxb_dom_node_t *node)
    void lxb_dom_node_insert_after(lxb_dom_node_t *to, lxb_dom_node_t *node)
    lxb_dom_text_t * lxb_dom_document_create_text_node(lxb_dom_document_t *document, const lxb_char_t *data, size_t len)
    void lxb_dom_node_simple_walk(lxb_dom_node_t *root, lxb_dom_node_simple_walker_f walker_cb, void *ctx)
    lxb_dom_node_t* lxb_dom_node_clone(lxb_dom_node_t *node, bint deep)


cdef extern from "lexbor/dom/interfaces/element.h" nogil:
    lxb_status_t lxb_dom_elements_by_tag_name(lxb_dom_element_t *root, lxb_dom_collection_t *collection,
                                              const lxb_char_t *qualified_name, size_t len)


cdef extern from "lexbor/dom/interfaces/document.h" nogil:
    lxb_html_document_t * lxb_html_document_destroy(lxb_html_document_t *document)


cdef extern from "lexbor/dom/collection.h" nogil:
    size_t lxb_dom_collection_length_noi(lxb_dom_collection_t *col)

    lxb_dom_element_t * lxb_dom_collection_element_noi(lxb_dom_collection_t *col, size_t idx)
    lxb_dom_collection_t * lxb_dom_collection_destroy(lxb_dom_collection_t *col, bint self_destroy)


cdef extern from "lexbor/css/css.h" nogil:
    ctypedef struct lxb_css_parser_t:
        lxb_css_memory_t* memory
    ctypedef struct lxb_css_syntax_tokenizer_t
    ctypedef struct lxb_css_memory_t

    lxb_css_parser_t * lxb_css_parser_create()
    lxb_status_t lxb_css_parser_init(lxb_css_parser_t *parser, lxb_css_syntax_tokenizer_t *tkz)
    lxb_css_parser_t * lxb_css_parser_destroy(lxb_css_parser_t *parser, bint self_destroy)
    lxb_css_memory_t * lxb_css_memory_destroy(lxb_css_memory_t *memory, bint self_destroy)
    void lxb_css_selector_list_destroy_memory(lxb_css_selector_list_t *list)


cdef extern from "lexbor/tag/tag.h" nogil:
    ctypedef enum lxb_tag_id_enum_t:
        LXB_TAG__UNDEF = 0x0000
        LXB_TAG__END_OF_FILE = 0x0001
        LXB_TAG__TEXT = 0x0002
        LXB_TAG__DOCUMENT = 0x0003
        LXB_TAG__EM_COMMENT = 0x0004
        LXB_TAG__EM_DOCTYPE = 0x0005
        LXB_TAG_A = 0x0006
        LXB_TAG_ABBR = 0x0007
        LXB_TAG_ACRONYM = 0x0008
        LXB_TAG_ADDRESS = 0x0009
        LXB_TAG_ALTGLYPH = 0x000a
        LXB_TAG_ALTGLYPHDEF = 0x000b
        LXB_TAG_ALTGLYPHITEM = 0x000c
        LXB_TAG_ANIMATECOLOR = 0x000d
        LXB_TAG_ANIMATEMOTION = 0x000e
        LXB_TAG_ANIMATETRANSFORM = 0x000f
        LXB_TAG_ANNOTATION_XML = 0x0010
        LXB_TAG_APPLET = 0x0011
        LXB_TAG_AREA = 0x0012
        LXB_TAG_ARTICLE = 0x0013
        LXB_TAG_ASIDE = 0x0014
        LXB_TAG_AUDIO = 0x0015
        LXB_TAG_B = 0x0016
        LXB_TAG_BASE = 0x0017
        LXB_TAG_BASEFONT = 0x0018
        LXB_TAG_BDI = 0x0019
        LXB_TAG_BDO = 0x001a
        LXB_TAG_BGSOUND = 0x001b
        LXB_TAG_BIG = 0x001c
        LXB_TAG_BLINK = 0x001d
        LXB_TAG_BLOCKQUOTE = 0x001e
        LXB_TAG_BODY = 0x001f
        LXB_TAG_BR = 0x0020
        LXB_TAG_BUTTON = 0x0021
        LXB_TAG_CANVAS = 0x0022
        LXB_TAG_CAPTION = 0x0023
        LXB_TAG_CENTER = 0x0024
        LXB_TAG_CITE = 0x0025
        LXB_TAG_CLIPPATH = 0x0026
        LXB_TAG_CODE = 0x0027
        LXB_TAG_COL = 0x0028
        LXB_TAG_COLGROUP = 0x0029
        LXB_TAG_DATA = 0x002a
        LXB_TAG_DATALIST = 0x002b
        LXB_TAG_DD = 0x002c
        LXB_TAG_DEL = 0x002d
        LXB_TAG_DESC = 0x002e
        LXB_TAG_DETAILS = 0x002f
        LXB_TAG_DFN = 0x0030
        LXB_TAG_DIALOG = 0x0031
        LXB_TAG_DIR = 0x0032
        LXB_TAG_DIV = 0x0033
        LXB_TAG_DL = 0x0034
        LXB_TAG_DT = 0x0035
        LXB_TAG_EM = 0x0036
        LXB_TAG_EMBED = 0x0037
        LXB_TAG_FEBLEND = 0x0038
        LXB_TAG_FECOLORMATRIX = 0x0039
        LXB_TAG_FECOMPONENTTRANSFER = 0x003a
        LXB_TAG_FECOMPOSITE = 0x003b
        LXB_TAG_FECONVOLVEMATRIX = 0x003c
        LXB_TAG_FEDIFFUSELIGHTING = 0x003d
        LXB_TAG_FEDISPLACEMENTMAP = 0x003e
        LXB_TAG_FEDISTANTLIGHT = 0x003f
        LXB_TAG_FEDROPSHADOW = 0x0040
        LXB_TAG_FEFLOOD = 0x0041
        LXB_TAG_FEFUNCA = 0x0042
        LXB_TAG_FEFUNCB = 0x0043
        LXB_TAG_FEFUNCG = 0x0044
        LXB_TAG_FEFUNCR = 0x0045
        LXB_TAG_FEGAUSSIANBLUR = 0x0046
        LXB_TAG_FEIMAGE = 0x0047
        LXB_TAG_FEMERGE = 0x0048
        LXB_TAG_FEMERGENODE = 0x0049
        LXB_TAG_FEMORPHOLOGY = 0x004a
        LXB_TAG_FEOFFSET = 0x004b
        LXB_TAG_FEPOINTLIGHT = 0x004c
        LXB_TAG_FESPECULARLIGHTING = 0x004d
        LXB_TAG_FESPOTLIGHT = 0x004e
        LXB_TAG_FETILE = 0x004f
        LXB_TAG_FETURBULENCE = 0x0050
        LXB_TAG_FIELDSET = 0x0051
        LXB_TAG_FIGCAPTION = 0x0052
        LXB_TAG_FIGURE = 0x0053
        LXB_TAG_FONT = 0x0054
        LXB_TAG_FOOTER = 0x0055
        LXB_TAG_FOREIGNOBJECT = 0x0056
        LXB_TAG_FORM = 0x0057
        LXB_TAG_FRAME = 0x0058
        LXB_TAG_FRAMESET = 0x0059
        LXB_TAG_GLYPHREF = 0x005a
        LXB_TAG_H1 = 0x005b
        LXB_TAG_H2 = 0x005c
        LXB_TAG_H3 = 0x005d
        LXB_TAG_H4 = 0x005e
        LXB_TAG_H5 = 0x005f
        LXB_TAG_H6 = 0x0060
        LXB_TAG_HEAD = 0x0061
        LXB_TAG_HEADER = 0x0062
        LXB_TAG_HGROUP = 0x0063
        LXB_TAG_HR = 0x0064
        LXB_TAG_HTML = 0x0065
        LXB_TAG_I = 0x0066
        LXB_TAG_IFRAME = 0x0067
        LXB_TAG_IMAGE = 0x0068
        LXB_TAG_IMG = 0x0069
        LXB_TAG_INPUT = 0x006a
        LXB_TAG_INS = 0x006b
        LXB_TAG_ISINDEX = 0x006c
        LXB_TAG_KBD = 0x006d
        LXB_TAG_KEYGEN = 0x006e
        LXB_TAG_LABEL = 0x006f
        LXB_TAG_LEGEND = 0x0070
        LXB_TAG_LI = 0x0071
        LXB_TAG_LINEARGRADIENT = 0x0072
        LXB_TAG_LINK = 0x0073
        LXB_TAG_LISTING = 0x0074
        LXB_TAG_MAIN = 0x0075
        LXB_TAG_MALIGNMARK = 0x0076
        LXB_TAG_MAP = 0x0077
        LXB_TAG_MARK = 0x0078
        LXB_TAG_MARQUEE = 0x0079
        LXB_TAG_MATH = 0x007a
        LXB_TAG_MENU = 0x007b
        LXB_TAG_META = 0x007c
        LXB_TAG_METER = 0x007d
        LXB_TAG_MFENCED = 0x007e
        LXB_TAG_MGLYPH = 0x007f
        LXB_TAG_MI = 0x0080
        LXB_TAG_MN = 0x0081
        LXB_TAG_MO = 0x0082
        LXB_TAG_MS = 0x0083
        LXB_TAG_MTEXT = 0x0084
        LXB_TAG_MULTICOL = 0x0085
        LXB_TAG_NAV = 0x0086
        LXB_TAG_NEXTID = 0x0087
        LXB_TAG_NOBR = 0x0088
        LXB_TAG_NOEMBED = 0x0089
        LXB_TAG_NOFRAMES = 0x008a
        LXB_TAG_NOSCRIPT = 0x008b
        LXB_TAG_OBJECT = 0x008c
        LXB_TAG_OL = 0x008d
        LXB_TAG_OPTGROUP = 0x008e
        LXB_TAG_OPTION = 0x008f
        LXB_TAG_OUTPUT = 0x0090
        LXB_TAG_P = 0x0091
        LXB_TAG_PARAM = 0x0092
        LXB_TAG_PATH = 0x0093
        LXB_TAG_PICTURE = 0x0094
        LXB_TAG_PLAINTEXT = 0x0095
        LXB_TAG_PRE = 0x0096
        LXB_TAG_PROGRESS = 0x0097
        LXB_TAG_Q = 0x0098
        LXB_TAG_RADIALGRADIENT = 0x0099
        LXB_TAG_RB = 0x009a
        LXB_TAG_RP = 0x009b
        LXB_TAG_RT = 0x009c
        LXB_TAG_RTC = 0x009d
        LXB_TAG_RUBY = 0x009e
        LXB_TAG_S = 0x009f
        LXB_TAG_SAMP = 0x00a0
        LXB_TAG_SCRIPT = 0x00a1
        LXB_TAG_SECTION = 0x00a2
        LXB_TAG_SELECT = 0x00a3
        LXB_TAG_SLOT = 0x00a4
        LXB_TAG_SMALL = 0x00a5
        LXB_TAG_SOURCE = 0x00a6
        LXB_TAG_SPACER = 0x00a7
        LXB_TAG_SPAN = 0x00a8
        LXB_TAG_STRIKE = 0x00a9
        LXB_TAG_STRONG = 0x00aa
        LXB_TAG_STYLE = 0x00ab
        LXB_TAG_SUB = 0x00ac
        LXB_TAG_SUMMARY = 0x00ad
        LXB_TAG_SUP = 0x00ae
        LXB_TAG_SVG = 0x00af
        LXB_TAG_TABLE = 0x00b0
        LXB_TAG_TBODY = 0x00b1
        LXB_TAG_TD = 0x00b2
        LXB_TAG_TEMPLATE = 0x00b3
        LXB_TAG_TEXTAREA = 0x00b4
        LXB_TAG_TEXTPATH = 0x00b5
        LXB_TAG_TFOOT = 0x00b6
        LXB_TAG_TH = 0x00b7
        LXB_TAG_THEAD = 0x00b8
        LXB_TAG_TIME = 0x00b9
        LXB_TAG_TITLE = 0x00ba
        LXB_TAG_TR = 0x00bb
        LXB_TAG_TRACK = 0x00bc
        LXB_TAG_TT = 0x00bd
        LXB_TAG_U = 0x00be
        LXB_TAG_UL = 0x00bf
        LXB_TAG_VAR = 0x00c0
        LXB_TAG_VIDEO = 0x00c1
        LXB_TAG_WBR = 0x00c2
        LXB_TAG_XMP = 0x00c3
        LXB_TAG__LAST_ENTRY = 0x00c4


cdef extern from "lexbor/selectors/selectors.h" nogil:
    ctypedef struct lxb_css_selectors_t

    ctypedef struct lxb_selectors_t
    ctypedef struct lxb_css_selector_list_t
    ctypedef struct lxb_css_selector_specificity_t
    ctypedef lxb_status_t (*lxb_selectors_cb_f)(lxb_dom_node_t *node, lxb_css_selector_specificity_t *spec, void *ctx)
    ctypedef enum lxb_selectors_opt_t:
        LXB_SELECTORS_OPT_DEFAULT = 0x00
        LXB_SELECTORS_OPT_MATCH_ROOT = 1 << 1
        LXB_SELECTORS_OPT_MATCH_FIRST = 1 << 2

    void lxb_selectors_opt_set(lxb_selectors_t *selectors, lxb_selectors_opt_t opt)
    lxb_css_selectors_t * lxb_css_selectors_create()
    lxb_status_t lxb_css_selectors_init(lxb_css_selectors_t *selectors)
    void lxb_css_parser_selectors_set(lxb_css_parser_t *parser, lxb_css_selectors_t *selectors)
    lxb_css_selector_list_t * lxb_css_selectors_parse(lxb_css_parser_t *parser, const lxb_char_t *data, size_t length)
    lxb_css_selectors_t * lxb_css_selectors_destroy(lxb_css_selectors_t *selectors, bint self_destroy)

    lxb_selectors_t * lxb_selectors_create()
    lxb_status_t lxb_selectors_init(lxb_selectors_t *selectors)
    lxb_selectors_t * lxb_selectors_destroy(lxb_selectors_t *selectors, bint self_destroy)
    lxb_status_t lxb_selectors_find(lxb_selectors_t *selectors, lxb_dom_node_t *root,
                                    lxb_css_selector_list_t *list, lxb_selectors_cb_f cb, void *ctx)
