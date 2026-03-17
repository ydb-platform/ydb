cdef class Match:
    cdef readonly Pattern re
    cdef readonly object string
    cdef readonly int pos
    cdef readonly int endpos
    cdef readonly tuple regs

    cdef StringPiece * matches
    cdef int encoded
    cdef int nmatches
    cdef int _lastindex
    cdef tuple _groups
    cdef dict _named_groups

    property lastindex:
        def __get__(self):
            return None if self._lastindex < 1 else self._lastindex

    property lastgroup:
        def __get__(self):
            if self._lastindex < 1:
                return None
            for name, n in self.re.groupindex.items():
                if n == self._lastindex:
                    return name
            return None

    def __init__(self, Pattern pattern_object, int num_groups):
        self._lastindex = -1
        self._groups = None
        self.pos = 0
        self.endpos = -1
        self.matches = new_StringPiece_array(num_groups + 1)
        self.nmatches = num_groups
        self.re = pattern_object

    cdef _init_groups(self):
        cdef list groups = []
        cdef int i
        cdef const char * last_end = NULL
        cdef const char * cur_end = NULL

        for i in range(self.nmatches):
            if self.matches[i].data() == NULL:
                groups.append(None)
            else:
                if i > 0:
                    cur_end = self.matches[i].data() + self.matches[i].length()

                    if last_end == NULL:
                        last_end = cur_end
                        self._lastindex = i
                    else:
                        # The rules for last group are a bit complicated:
                        # if two groups end at the same point, the earlier one
                        # is considered last, so we don't switch our selection
                        # unless the end point has moved.
                        if cur_end > last_end:
                            last_end = cur_end
                            self._lastindex = i
                groups.append(
                        self.matches[i].data()[:self.matches[i].length()])
        self._groups = tuple(groups)

    cdef bytes _group(self, object groupnum):
        cdef int idx
        if isinstance(groupnum, int):
            idx = groupnum
            if idx > self.nmatches - 1:
                raise IndexError("no such group %d; available groups: %r"
                        % (idx, list(range(self.nmatches))))
            return self._groups[idx]
        groupdict = self._groupdict()
        if groupnum not in groupdict:
            raise IndexError("no such group %r; available groups: %r"
                    % (groupnum, list(groupdict)))
        return groupdict[groupnum]

    cdef dict _groupdict(self):
        if self._named_groups is None:
            self._named_groups = {name: self._groups[n]
                    for name, n in self.re.groupindex.items()}
        return self._named_groups

    def groups(self, default=None):
        if self.encoded:
            return tuple([default if g is None else g.decode('utf8')
                    for g in self._groups[1:]])
        return tuple([default if g is None else g
                for g in self._groups[1:]])

    def group(self, *args):
        if len(args) == 0:
            groupnum = 0
        elif len(args) == 1:
            groupnum = args[0]
        else:  # len(args) > 1:
            return tuple([self.group(i) for i in args])
        if self.encoded:
            result = self._group(groupnum)
            return None if result is None else result.decode('utf8')
        return self._group(groupnum)

    def __getitem__(self, key):
        return self.group(key)

    def groupdict(self):
        result = self._groupdict()
        if self.encoded:
            return {a: None if b is None else b.decode('utf8')
                    for a, b in result.items()}
        return result

    def expand(self, object template):
        """Expand a template with groups."""
        cdef bytearray result = bytearray()
        if isinstance(template, unicode):
            if not self.encoded:
                raise ValueError(
                        'cannot expand unicode template on bytes pattern')
            templ = template.encode('utf8')
        else:
            if self.encoded:
                raise ValueError(
                        'cannot expand bytes template on unicode pattern')
            templ = bytes(template)
        self._expand(templ, result)
        return result.decode('utf8') if self.encoded else bytes(result)

    cdef _expand(self, bytes templ, bytearray result):
        """Expand template by appending to an existing bytearray.
        Everything remains UTF-8 encoded."""
        cdef char * cstring
        cdef int n = 0, prev = 0, size

        # NB: cstring is used to get single characters, to avoid difference in
        # Python 2/3 behavior of bytes objects.
        cstring = templ
        size = len(templ)
        while True:
            prev = n
            n = templ.find(b'\\', prev)
            if n == -1:
                result.extend(templ[prev:])
                break
            result.extend(templ[prev:n])
            n += 1
            if (n + 2 < size and cstring[n] == b'x'
                    and ishex(cstring[n + 1]) and ishex(cstring[n + 2])):
                # hex char reference \x1f
                result.append(int(templ[n + 1:n + 3], base=16) & 255)
                n += 3
            elif (n + 2 < size and isoct(cstring[n]) and isoct(cstring[n + 1])
                    and isoct(cstring[n + 2])):
                # octal char reference \123
                result.append(int(templ[n:n + 3], base=8) & 255)
                n += 3
            elif cstring[n] == b'0':
                if n + 1 < size and isoct(cstring[n + 1]):
                    # 2 character octal: \01
                    result.append(int(templ[n:n + 2], base=8))
                    n += 2
                else:  # nul-terminator literal \0
                    result.append(b'\0')
                    n += 1
            elif b'0' <= cstring[n] <= b'9':  # numeric group reference
                if n + 1 < size and isdigit(cstring[n + 1]):
                    # 2 digit group ref \12
                    groupno = int(templ[n:n + 2])
                    n += 2
                else:
                    # 1 digit group ref \1
                    groupno = int(templ[n:n + 1])
                    n += 1
                if groupno <= self.re.groups:
                    groupval = self._group(groupno)
                    if groupval is not None:
                        result.extend(groupval)
                else:
                    raise RegexError('invalid group reference.')
            elif cstring[n] == b'g':  # named group reference
                n += 1
                if n >= size or cstring[n] != b'<':
                    raise RegexError('missing group name')
                n += 1
                start = n
                while cstring[n] != b'>':
                    if not isident(cstring[n]):
                        raise RegexError('bad character in group name')
                    n += 1
                    if n >= size:
                        raise RegexError('unterminated group name')
                if templ[start:n].isdigit():
                    name = int(templ[start:n])
                elif isdigit(cstring[start]):
                    raise RegexError('bad character in group name')
                else:
                    name = templ[start:n]
                    if self.encoded:
                        name = name.decode('utf8')
                groupval = self._group(name)
                if groupval is not None:
                    result.extend(groupval)
                n += 1
            else:
                if cstring[n] == b'n':
                    result.append(b'\n')
                elif cstring[n] == b'r':
                    result.append(b'\r')
                elif cstring[n] == b't':
                    result.append(b'\t')
                elif cstring[n] == b'v':
                    result.append(b'\v')
                elif cstring[n] == b'f':
                    result.append(b'\f')
                elif cstring[n] == b'a':
                    result.append(b'\a')
                elif cstring[n] == b'b':
                    result.append(b'\b')
                elif cstring[n] == b'\\':
                    result.append(b'\\')
                else:  # copy verbatim
                    result.append(b'\\')
                    result.append(cstring[n])
                n += 1
        return bytes(result)

    def start(self, group=0):
        return self.span(group)[0]

    def end(self, group=0):
        return self.span(group)[1]

    def span(self, group=0):
        if isinstance(group, int):
            if group > len(self.regs):
                raise IndexError("no such group %d; available groups: %r"
                        % (group, list(range(len(self.regs)))))
            return self.regs[group]
        else:
            self._groupdict()
            if group not in self.re.groupindex:
                raise IndexError("no such group %r; available groups: %r"
                        % (group, list(self.re.groupindex)))
            return self.regs[self.re.groupindex[group]]

    cdef _make_spans(self, char * cstring, int size, int * cpos, int * upos):
        cdef int start, end
        cdef StringPiece * piece

        spans = []
        for i in range(self.nmatches):
            if self.matches[i].data() == NULL:
                spans.append((-1, -1))
            else:
                piece = &self.matches[i]
                if piece.data() == NULL:
                    return (-1, -1)
                start = piece.data() - cstring
                end = start + piece.length()
                spans.append((start, end))

        if self.encoded == 2:
            spans = self._convert_spans(spans, cstring, size, cpos, upos)

        self.regs = tuple(spans)

    cdef list _convert_spans(self, spans,
            char * cstring, int size, int * cpos, int * upos):
        cdef map[int, int] positions
        cdef int x, y
        for x, y in spans:
            positions[x] = x
            positions[y] = y
        unicodeindices(positions, cstring, size, cpos, upos)
        return [(positions[x], positions[y]) for x, y in spans]

    def __dealloc__(self):
        delete_StringPiece_array(self.matches)

    def __repr__(self):
        return '<re2.Match object; span=%r, match=%r>' % (
                self.span(), self.group())
