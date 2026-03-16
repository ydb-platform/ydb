cdef extern from *:
    ctypedef char* const_char_ptr "const char*"

cdef extern from "uchardet.h":
    ctypedef void* uchardet_t
    cdef uchardet_t uchardet_new()
    cdef void uchardet_delete(uchardet_t ud)
    cdef int uchardet_handle_data(uchardet_t ud, const_char_ptr data, int length)
    cdef void uchardet_data_end(uchardet_t ud)
    cdef void uchardet_reset(uchardet_t ud)
    cdef const_char_ptr uchardet_get_charset(uchardet_t ud)
    cdef float uchardet_get_confidence(uchardet_t ud)

def detect_with_confidence(bytes msg):
    cdef int length = len(msg)
    
    cdef uchardet_t ud = uchardet_new()

    cdef int result = uchardet_handle_data(ud, msg, length)
    if result == -1:
        uchardet_delete(ud)
        raise Exception("Handle data error")

    uchardet_data_end(ud)

    cdef bytes detected_charset = uchardet_get_charset(ud)
    cdef float detected_confidence = uchardet_get_confidence(ud)
    uchardet_delete(ud)

    if detected_charset:
        return detected_charset, detected_confidence

    return None, None

cdef class UniversalDetector:
    cdef uchardet_t _ud
    cdef int _done
    cdef int _closed
    cdef bytes _detected_charset
    cdef float _detected_confidence

    def __init__(self):
        self._ud = uchardet_new()
        self._done = 0
        self._closed = 0
        self._detected_charset = b""
        self._detected_confidence = 0.0

    def reset(self):
        if not self._closed:
            self._done = 0
            self._closed = 0
            self._detected_charset = b""
            self._detected_confidence = 0.0
            uchardet_reset(self._ud)

    def feed(self, bytes msg):
        cdef int length
        cdef int result

        if self._closed:
            return

        length = len(msg)
        if length > 0:
            result = uchardet_handle_data(self._ud, msg, length)

            if result == -1:
                self._closed = 1
                uchardet_delete(self._ud)
                raise Exception("Handle data error")
            elif result == 0:
                self._done = 1

            self._detected_charset = uchardet_get_charset(self._ud)
            self._detected_confidence = uchardet_get_confidence(self._ud)

    def close(self):
        if not self._closed:
            uchardet_data_end(self._ud)
            self._detected_charset = uchardet_get_charset(self._ud)
            self._detected_confidence = uchardet_get_confidence(self._ud)

            uchardet_delete(self._ud)
            self._closed = 1

    @property
    def done(self):
        return bool(self._done)

    @property
    def result(self):
        if len(self._detected_charset):
            return self._detected_charset, self._detected_confidence
        else:
            return None, None
