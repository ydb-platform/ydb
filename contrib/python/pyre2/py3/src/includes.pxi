cimport cpython.unicode
from libcpp.map cimport map
from libcpp.string cimport string as cpp_string
from cython.operator cimport postincrement, dereference
from cpython.buffer cimport Py_buffer, PyBUF_SIMPLE, PyObject_CheckBuffer, \
        PyObject_GetBuffer, PyBuffer_Release
from cpython.version cimport PY_MAJOR_VERSION


cdef extern from *:
    cdef void emit_if_narrow_unicode "#if !defined(Py_UNICODE_WIDE) && PY_VERSION_HEX < 0x03030000 //" ()
    cdef void emit_endif "#endif //" ()


cdef extern from "re2/stringpiece.h" namespace "re2":
    cdef cppclass StringPiece:
        StringPiece()
        StringPiece(const char *)
        StringPiece(const char *, int)
        const char * data()
        int copy(char * buf, size_t n, size_t pos)
        int length()


cdef extern from "re2/re2.h" namespace "re2":
    cdef enum Anchor:
        UNANCHORED "RE2::UNANCHORED"
        ANCHOR_START "RE2::ANCHOR_START"
        ANCHOR_BOTH "RE2::ANCHOR_BOTH"

    ctypedef Anchor re2_Anchor "RE2::Anchor"

    cdef enum ErrorCode:
        NoError "RE2::NoError"
        ErrorInternal "RE2::ErrorInternal"
        # Parse errors
        ErrorBadEscape "RE2::ErrorBadEscape"          # bad escape sequence
        ErrorBadCharClass "RE2::ErrorBadCharClass"       # bad character class
        ErrorBadCharRange "RE2::ErrorBadCharRange"       # bad character class range
        ErrorMissingBracket "RE2::ErrorMissingBracket"     # missing closing ]
        ErrorMissingParen   "RE2::ErrorMissingParen"       # missing closing )
        ErrorTrailingBackslash "RE2::ErrorTrailingBackslash"  # trailing \ at end of regexp
        ErrorRepeatArgument "RE2::ErrorRepeatArgument"     # repeat argument missing, e.g. "*"
        ErrorRepeatSize "RE2::ErrorRepeatSize"         # bad repetition argument
        ErrorRepeatOp "RE2::ErrorRepeatOp"           # bad repetition operator
        ErrorBadPerlOp "RE2::ErrorBadPerlOp"          # bad perl operator
        ErrorBadUTF8 "RE2::ErrorBadUTF8"            # invalid UTF-8 in regexp
        ErrorBadNamedCapture "RE2::ErrorBadNamedCapture"    # bad named capture group
        ErrorPatternTooLarge "RE2::ErrorPatternTooLarge"    # pattern too large (compile failed)

    cdef enum Encoding:
        EncodingUTF8 "RE2::Options::EncodingUTF8"
        EncodingLatin1 "RE2::Options::EncodingLatin1"

    ctypedef Encoding re2_Encoding "RE2::Options::Encoding"

    cdef cppclass Options "RE2::Options":
        Options()
        void set_posix_syntax(int b)
        void set_longest_match(int b)
        void set_log_errors(int b)
        void set_max_mem(int m)
        void set_literal(int b)
        void set_never_nl(int b)
        void set_case_sensitive(int b)
        void set_perl_classes(int b)
        void set_word_boundary(int b)
        void set_one_line(int b)
        int case_sensitive()
        void set_encoding(re2_Encoding encoding)

    cdef cppclass RE2:
        RE2(const StringPiece pattern, Options option) nogil
        RE2(const StringPiece pattern) nogil
        int Match(const StringPiece text, int startpos, int endpos,
                Anchor anchor, StringPiece * match, int nmatch) nogil
        int Replace(cpp_string *str, const RE2 pattern,
                const StringPiece rewrite) nogil
        int GlobalReplace(cpp_string *str, const RE2 pattern,
                const StringPiece rewrite) nogil
        int NumberOfCapturingGroups()
        int ok()
        const cpp_string pattern()
        cpp_string error()
        ErrorCode error_code()
        const map[cpp_string, int]& NamedCapturingGroups()

    # hack for static methods
    cdef int Replace "RE2::Replace"(
            cpp_string *str, const RE2 pattern,
            const StringPiece rewrite) nogil
    cdef int GlobalReplace "RE2::GlobalReplace"(
            cpp_string *str,
            const RE2 pattern,
            const StringPiece rewrite) nogil


cdef extern from "_re2macros.h":
    StringPiece * new_StringPiece_array(int) nogil


cdef extern from *:
    # StringPiece * new_StringPiece_array "new re2::StringPiece[n]" (int) nogil
    void delete_StringPiece_array "delete[]" (StringPiece *) nogil
