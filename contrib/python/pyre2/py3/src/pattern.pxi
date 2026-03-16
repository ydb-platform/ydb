cdef class Pattern:
    cdef readonly object pattern  # original pattern in Python format
    cdef readonly int flags
    cdef readonly int groups  # number of groups
    cdef readonly dict groupindex  # name => group number
    cdef object __weakref__

    cdef bint encoded  # True if this was originally a Unicode pattern
    cdef RE2 * re_pattern

    def search(self, object string, int pos=0, int endpos=-1):
        """Scan through string looking for a match, and return a corresponding
        Match instance. Return None if no position in the string matches."""
        return self._search(string, pos, endpos, UNANCHORED)

    def match(self, object string, int pos=0, int endpos=-1):
        """Matches zero or more characters at the beginning of the string."""
        return self._search(string, pos, endpos, ANCHOR_START)

    def fullmatch(self, object string, int pos=0, int endpos=-1):
        """"fullmatch(string[, pos[, endpos]]) --> Match object or None."

        Matches the entire string."""
        return self._search(string, pos, endpos, ANCHOR_BOTH)

    cdef _search(self, object string, int pos, int endpos,
            re2_Anchor anchoring):
        """Scan through string looking for a match, and return a corresponding
        Match instance. Return None if no position in the string matches."""
        cdef char * cstring
        cdef Py_ssize_t size
        cdef Py_buffer buf
        cdef int retval
        cdef int encoded = 0
        cdef StringPiece * sp
        cdef Match m = Match(self, self.groups + 1)
        cdef int cpos = 0, upos = pos

        if 0 <= endpos <= pos:
            return None

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        try:
            if encoded == 2 and (pos or endpos != -1):
                utf8indices(cstring, size, &pos, &endpos)
                cpos = pos
            if pos > size:
                return None
            if 0 <= endpos < size:
                size = endpos

            sp = new StringPiece(cstring, size)
            with nogil:
                retval = self.re_pattern.Match(
                        sp[0],
                        pos,
                        size,
                        anchoring,
                        m.matches,
                        self.groups + 1)
            del sp
            if retval == 0:
                return None

            m.encoded = encoded
            m.nmatches = self.groups + 1
            m.string = string
            m.pos = pos
            if endpos == -1:
                m.endpos = size
            else:
                m.endpos = endpos
            m._make_spans(cstring, size, &cpos, &upos)
            m._init_groups()
        finally:
            release_cstring(&buf)
        return m

    def contains(self, object string, int pos=0, int endpos=-1):
        """"contains(string[, pos[, endpos]]) --> bool."

        Scan through string looking for a match, and return True or False."""
        cdef char * cstring
        cdef Py_ssize_t size
        cdef Py_buffer buf
        cdef int retval
        cdef int encoded = 0
        cdef StringPiece * sp

        if 0 <= endpos <= pos:
            return False

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        try:
            if encoded == 2 and (pos or endpos != -1):
                utf8indices(cstring, size, &pos, &endpos)
            if pos > size:
                return False
            if 0 <= endpos < size:
                size = endpos

            sp = new StringPiece(cstring, size)
            with nogil:
                retval = self.re_pattern.Match(
                        sp[0],
                        pos,
                        size,
                        UNANCHORED,
                        NULL,
                        0)
            del sp
        finally:
            release_cstring(&buf)
        return retval != 0

    def count(self, object string, int pos=0, int endpos=-1):
        """Return number of non-overlapping matches of pattern in string."""
        cdef char * cstring
        cdef Py_ssize_t size
        cdef Py_buffer buf
        cdef int retval
        cdef int encoded = 0
        cdef int result = 0
        cdef StringPiece * sp = NULL
        cdef StringPiece * matches = NULL

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        try:
            if encoded == 2 and (pos or endpos != -1):
                utf8indices(cstring, size, &pos, &endpos)
            if pos > size:
                return 0
            if 0 <= endpos < size:
                size = endpos

            sp = new StringPiece(cstring, size)
            matches = new_StringPiece_array(1)
            try:
                while True:
                    with nogil:
                        retval = self.re_pattern.Match(
                                sp[0],
                                pos,
                                size,
                                UNANCHORED,
                                matches,
                                1)
                    if retval == 0:
                        break
                    result += 1
                    if pos == size:
                        break
                    # offset the pos to move to the next point
                    pos = matches[0].data() - cstring + (
                            matches[0].length() or 1)
            finally:
                del sp
                delete_StringPiece_array(matches)
        finally:
            release_cstring(&buf)
        return result

    def findall(self, object string, int pos=0, int endpos=-1):
        """Return all non-overlapping matches of pattern in string as a list
        of strings."""
        cdef char * cstring
        cdef Py_ssize_t size
        cdef Py_buffer buf
        cdef int encoded = 0
        cdef int retval
        cdef list resultlist = []
        cdef StringPiece * sp = NULL
        cdef StringPiece * matches = NULL

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        try:
            if encoded == 2 and (pos or endpos != -1):
                utf8indices(cstring, size, &pos, &endpos)
            if pos > size:
                return []
            if 0 <= endpos < size:
                size = endpos

            sp = new StringPiece(cstring, size)
            matches = new_StringPiece_array(self.groups + 1)

            while True:
                with nogil:
                    retval = self.re_pattern.Match(
                            sp[0],
                            pos,
                            size,
                            UNANCHORED,
                            matches,
                            self.groups + 1)
                if retval == 0:
                    break
                if self.groups > 1:
                    if encoded:
                        resultlist.append(tuple([
                            '' if matches[i].data() is NULL else
                            matches[i].data()[:matches[i].length()
                                ].decode('utf8')
                            for i in range(1, self.groups + 1)]))
                    else:
                        resultlist.append(tuple([
                            b'' if matches[i].data() is NULL
                            else matches[i].data()[:matches[i].length()]
                            for i in range(1, self.groups + 1)]))
                else:  # 0 or 1 group; return list of strings
                    if encoded:
                        resultlist.append(matches[self.groups].data()[
                            :matches[self.groups].length()].decode('utf8'))
                    else:
                        resultlist.append(matches[self.groups].data()[
                            :matches[self.groups].length()])
                if pos == size:
                    break
                # offset the pos to move to the next point
                pos = matches[0].data() - cstring + (matches[0].length() or 1)
        finally:
            del sp
            delete_StringPiece_array(matches)
            release_cstring(&buf)
        return resultlist

    def finditer(self, object string, int pos=0, int endpos=-1):
        """Yield all non-overlapping matches of pattern in string as Match
        objects."""
        result = iter(self._finditer(string, pos, endpos))
        next(result)  # dummy value to raise error before start of generator
        return result

    def _finditer(self, object string, int pos=0, int endpos=-1):
        cdef char * cstring
        cdef Py_ssize_t size
        cdef Py_buffer buf
        cdef int retval
        cdef StringPiece * sp = NULL
        cdef Match m
        cdef int encoded = 0
        cdef int cpos = 0, upos = pos

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        try:
            if encoded == 2 and (pos or endpos != -1):
                utf8indices(cstring, size, &pos, &endpos)
                cpos = pos
            if pos > size:
                return
            if 0 <= endpos < size:
                size = endpos

            sp = new StringPiece(cstring, size)

            yield
            while True:
                m = Match(self, self.groups + 1)
                m.string = string
                with nogil:
                    retval = self.re_pattern.Match(
                            sp[0],
                            pos,
                            size,
                            UNANCHORED,
                            m.matches,
                            self.groups + 1)
                if retval == 0:
                    break
                m.encoded = encoded
                m.nmatches = self.groups + 1
                m.pos = pos
                if endpos == -1:
                    m.endpos = size
                else:
                    m.endpos = endpos
                m._make_spans(cstring, size, &cpos, &upos)
                m._init_groups()
                yield m
                if pos == size:
                    break
                # offset the pos to move to the next point
                pos = m.matches[0].data() - cstring + (
                        m.matches[0].length() or 1)
        finally:
            del sp
            release_cstring(&buf)

    def split(self, string, int maxsplit=0):
        """split(string[, maxsplit = 0]) --> list

        Split a string by the occurrences of the pattern."""
        cdef char * cstring
        cdef Py_ssize_t size
        cdef int retval
        cdef int pos = 0
        cdef int lookahead = 0
        cdef int num_split = 0
        cdef StringPiece * sp
        cdef StringPiece * matches
        cdef list resultlist = []
        cdef int encoded = 0
        cdef Py_buffer buf

        if maxsplit < 0:
            maxsplit = 0

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        matches = new_StringPiece_array(self.groups + 1)
        sp = new StringPiece(cstring, size)
        try:

            while True:
                with nogil:
                    retval = self.re_pattern.Match(
                            sp[0],
                            pos + lookahead,
                            size,
                            UNANCHORED,
                            matches,
                            self.groups + 1)
                if retval == 0:
                    break

                match_start = matches[0].data() - cstring
                match_end = match_start + matches[0].length()

                # If an empty match, just look ahead until you find something
                if match_start == match_end:
                    if pos + lookahead == size:
                        break
                    lookahead += 1
                    continue

                if encoded:
                    resultlist.append(
                            char_to_unicode(&sp.data()[pos], match_start - pos))
                else:
                    resultlist.append(sp.data()[pos:match_start])
                if self.groups > 0:
                    for group in range(self.groups):
                        if matches[group + 1].data() == NULL:
                            resultlist.append(None)
                        else:
                            if encoded:
                                resultlist.append(char_to_unicode(
                                        matches[group + 1].data(),
                                        matches[group + 1].length()))
                            else:
                                resultlist.append(matches[group + 1].data()[:
                                        matches[group + 1].length()])

                # offset the pos to move to the next point
                pos = match_end
                lookahead = 0

                num_split += 1
                if maxsplit and num_split >= maxsplit:
                    break

            if encoded:
                resultlist.append(
                        char_to_unicode(&sp.data()[pos], sp.length() - pos))
            else:
                resultlist.append(sp.data()[pos:size])
        finally:
            del sp
            delete_StringPiece_array(matches)
            release_cstring(&buf)
        return resultlist

    def sub(self, repl, string, int count=0):
        """sub(repl, string[, count = 0]) --> newstring

        Return the string obtained by replacing the leftmost non-overlapping
        occurrences of pattern in string by the replacement repl."""
        cdef int num_repl = 0
        return self._subn(repl, string, count, &num_repl)

    def subn(self, repl, string, int count=0):
        """subn(repl, string[, count = 0]) --> (newstring, number of subs)

        Return the tuple (new_string, number_of_subs_made) found by replacing
        the leftmost non-overlapping occurrences of pattern with the
        replacement repl."""
        cdef int num_repl = 0
        result = self._subn(repl, string, count, &num_repl)
        return result, num_repl

    cdef _subn(self, repl, string, int count, int *num_repl):
        cdef bytes repl_b
        cdef char * cstring
        cdef object result
        cdef Py_ssize_t size
        cdef StringPiece * sp = NULL
        cdef cpp_string * input_str = NULL
        cdef int string_encoded = 0
        cdef int repl_encoded = 0

        if callable(repl):
            # This is a callback, so use the custom function
            return self._subn_callback(repl, string, count, num_repl)

        repl_b = unicode_to_bytes(repl, &repl_encoded, self.encoded)
        if not repl_encoded and not isinstance(repl, bytes):
            repl_b = bytes(repl)  # coerce buffer to bytes object

        if count > 1 or <char>b'\\' in repl_b:
            # Limit on number of substitutions or replacement string contains
            # escape sequences; handle with Match.expand() implementation.
            # RE2 does support simple numeric group references \1, \2,
            # but the number of differences with Python behavior is
            # non-trivial.
            return self._subn_expand(repl_b, string, count, num_repl)
        try:
            cstring = repl_b
            size = len(repl_b)
            sp = new StringPiece(cstring, size)

            bytestr = unicode_to_bytes(string, &string_encoded, self.encoded)
            if not string_encoded and not isinstance(bytestr, bytes):
                bytestr = bytes(bytestr)  # coerce buffer to bytes object
            input_str = new cpp_string(<char *>bytestr, len(bytestr))
            # NB: RE2 treats unmatched groups in repl as empty string;
            # Python raises an error.
            with nogil:
                if count == 0:
                    num_repl[0] = GlobalReplace(
                            input_str, self.re_pattern[0], sp[0])
                elif count == 1:
                    num_repl[0] = Replace(
                            input_str, self.re_pattern[0], sp[0])

            if string_encoded or (repl_encoded and num_repl[0] > 0):
                result = cpp_to_unicode(input_str[0])
            else:
                result = cpp_to_bytes(input_str[0])
        finally:
            del input_str, sp
        return result

    cdef _subn_callback(self, callback, string, int count, int * num_repl):
        # This function is probably the hardest to implement correctly.
        # This is my first attempt, but if anybody has a better solution,
        # please help out.
        cdef char * cstring
        cdef Py_ssize_t size
        cdef Py_buffer buf
        cdef int retval
        cdef int prevendpos = -1
        cdef int endpos = 0
        cdef int pos = 0
        cdef int encoded = 0
        cdef StringPiece * sp
        cdef Match m
        cdef bytearray result = bytearray()
        cdef int cpos = 0, upos = 0

        if count < 0:
            count = 0

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        sp = new StringPiece(cstring, size)
        try:
            while True:
                m = Match(self, self.groups + 1)
                m.string = string
                with nogil:
                    retval = self.re_pattern.Match(
                            sp[0],
                            pos,
                            size,
                            UNANCHORED,
                            m.matches,
                            self.groups + 1)
                if retval == 0:
                    break

                endpos = m.matches[0].data() - cstring
                if endpos == prevendpos:
                    endpos += 1
                    if endpos > size:
                        break
                prevendpos = endpos
                result.extend(sp.data()[pos:endpos])
                pos = endpos + m.matches[0].length()

                m.encoded = encoded
                m.nmatches = self.groups + 1
                m._make_spans(cstring, size, &cpos, &upos)
                m._init_groups()
                tmp = callback(m)
                if tmp:
                    result.extend(tmp.encode('utf8') if encoded else tmp)
                else:
                    result.extend(b'')

                num_repl[0] += 1
                if count and num_repl[0] >= count:
                    break
            result.extend(sp.data()[pos:size])
        finally:
            del sp
            release_cstring(&buf)
        return result.decode('utf8') if encoded else bytes(result)

    cdef _subn_expand(self, bytes repl, string, int count, int * num_repl):
        """Perform ``count`` substitutions with replacement string and
        Match.expand."""
        cdef char * cstring
        cdef Py_ssize_t size
        cdef Py_buffer buf
        cdef int retval
        cdef int prevendpos = -1
        cdef int endpos = 0
        cdef int pos = 0
        cdef int encoded = 0
        cdef StringPiece * sp
        cdef Match m
        cdef bytearray result = bytearray()

        if count < 0:
            count = 0

        bytestr = unicode_to_bytes(string, &encoded, self.encoded)
        if pystring_to_cstring(bytestr, &cstring, &size, &buf) == -1:
            raise TypeError('expected string or buffer')
        sp = new StringPiece(cstring, size)
        try:
            while True:
                m = Match(self, self.groups + 1)
                m.string = string
                with nogil:
                    retval = self.re_pattern.Match(
                            sp[0],
                            pos,
                            size,
                            UNANCHORED,
                            m.matches,
                            self.groups + 1)
                if retval == 0:
                    break

                endpos = m.matches[0].data() - cstring
                if endpos == prevendpos:
                    endpos += 1
                    if endpos > size:
                        break
                prevendpos = endpos
                result.extend(sp.data()[pos:endpos])
                pos = endpos + m.matches[0].length()

                m.encoded = encoded
                m.nmatches = self.groups + 1
                m._init_groups()
                m._expand(repl, result)

                num_repl[0] += 1
                if count and num_repl[0] >= count:
                    break
            result.extend(sp.data()[pos:size])
        finally:
            del sp
            release_cstring(&buf)
        return result.decode('utf8') if encoded else bytes(result)

    def scanner(self, arg):
        return re.compile(self.pattern).scanner(arg)
        # raise NotImplementedError

    def _dump_pattern(self):
        cdef cpp_string s = self.re_pattern.pattern()
        if self.encoded:
            return cpp_to_bytes(s).decode('utf8')
        return cpp_to_bytes(s)

    def __repr__(self):
        if self.flags == 0:
            return 're2.compile(%r)' % self.pattern
        return 're2.compile(%r, %r)' % (self.pattern, self.flags)

    def __reduce__(self):
        return (compile, (self.pattern, self.flags))

    def __dealloc__(self):
        del self.re_pattern


class FallbackPattern:
    """A wrapper for non-re2 ``Pattern`` to support the extra methods defined
    by re2 (contains, count)."""
    def __init__(self, pattern, flags=None):
        self._pattern = fallback_module.compile(pattern, flags)
        self.pattern = pattern
        self.flags = flags
        self.groupindex = self._pattern.groupindex
        self.groups = self._pattern.groups

    def contains(self, string):
        return bool(self._pattern.search(string))

    def count(self, string, pos=0, endpos=9223372036854775807):
        return len(self._pattern.findall(string, pos, endpos))

    def findall(self, string, pos=0, endpos=9223372036854775807):
        return self._pattern.findall(string, pos, endpos)

    def finditer(self, string, pos=0, endpos=9223372036854775807):
        return self._pattern.finditer(string, pos, endpos)

    def fullmatch(self, string, pos=0, endpos=9223372036854775807):
        return self._pattern.fullmatch(string, pos, endpos)

    def match(self, string, pos=0, endpos=9223372036854775807):
        return self._pattern.match(string, pos, endpos)

    def scanner(self, string, pos=0, endpos=9223372036854775807):
        return self._pattern.scanner(string, pos, endpos)

    def search(self, string, pos=0, endpos=9223372036854775807):
        return self._pattern.search(string, pos, endpos)

    def split(self, string, maxsplit=0):
        return self._pattern.split(string, maxsplit)

    def sub(self, repl, string, count=0):
        return self._pattern.sub(repl, string, count)

    def subn(self, repl, string, count=0):
        return self._pattern.subn(repl, string, count)

    def __repr__(self):
        return repr(self._pattern)

    def __reduce__(self):
        return (self.__class__, (self.pattern, self.flags))
