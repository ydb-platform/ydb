
def compile(pattern, int flags=0, int max_mem=8388608):
    cachekey = (type(pattern), pattern, flags, current_notification)
    if cachekey in _cache:
        return _cache[cachekey]
    p = _compile(pattern, flags, max_mem)

    if len(_cache) >= _MAXCACHE:
        _cache.popitem()
    _cache[cachekey] = p
    return p


def _compile(object pattern, int flags=0, int max_mem=8388608):
    """Compile a regular expression pattern, returning a pattern object."""
    def fallback(pattern, flags, error_msg):
        """Raise error, warn, or simply return fallback from re-compatible module."""
        if current_notification == FALLBACK_EXCEPTION:
            raise RegexError(error_msg)
        elif current_notification == FALLBACK_WARNING:
            warnings.warn("WARNING: Using %s module. Reason: %s" % (fallback_module.__name__, error_msg))
        try:
            result = FallbackPattern(pattern, flags)
        except re.error as err:
            raise RegexError(*err.args)
        return result

    cdef StringPiece * s
    cdef Options opts
    cdef int error_code
    cdef int encoded = 0
    cdef object original_pattern

    if isinstance(pattern, (Pattern, SREPattern)):
        if flags:
            raise ValueError(
                    'Cannot process flags argument with a compiled pattern')
        return pattern

    original_pattern = pattern
    if flags & _L:
        return fallback(original_pattern, flags, "re.LOCALE not supported")
    pattern = unicode_to_bytes(pattern, &encoded, -1)
    newflags = flags
    if not encoded and flags & _U:  # re.UNICODE
        pass  # can use UNICODE with bytes pattern, but assumes valid UTF-8
        # raise ValueError("can't use UNICODE flag with a bytes pattern")
    elif encoded and not (flags & ASCII):  # re.ASCII (not in Python 2)
        newflags = flags | _U  # re.UNICODE
    elif encoded and flags & ASCII:
        newflags = flags & ~_U  # re.UNICODE
    try:
        pattern = _prepare_pattern(pattern, newflags)
    except BackreferencesException:
        return fallback(original_pattern, flags, "Backreferences not supported")
    except CharClassProblemException:
        return fallback(original_pattern, flags,
                "\W and \S not supported inside character classes")

    # Set the options given the flags above.
    if flags & _I:
        opts.set_case_sensitive(0);

    opts.set_max_mem(max_mem)
    opts.set_log_errors(0)
    if flags & _U or encoded:
        opts.set_encoding(EncodingUTF8)
    else:  # re.UNICODE flag not passed, and pattern is bytes,
        # so allow matching of arbitrary byte sequences.
        opts.set_encoding(EncodingLatin1)

    s = new StringPiece(<char *><bytes>pattern, len(pattern))

    cdef RE2 *re_pattern
    with nogil:
         re_pattern = new RE2(s[0], opts)

    if not re_pattern.ok():
        # Something went wrong with the compilation.
        del s
        error_msg = cpp_to_unicode(re_pattern.error())
        error_code = re_pattern.error_code()
        del re_pattern
        if current_notification == FALLBACK_EXCEPTION:
            # Raise an exception regardless of the type of error.
            raise RegexError(error_msg)
        elif error_code not in (ErrorBadPerlOp, ErrorRepeatSize,
                # ErrorBadEscape,
                ErrorRepeatOp, ErrorPatternTooLarge):
            # Raise an error because these will not be fixed by using the
            # ``re`` module.
            raise RegexError(error_msg)
        elif current_notification == FALLBACK_WARNING:
            warnings.warn("WARNING: Using %s module. Reason: %s" % (fallback_module.__name__, error_msg))
        return FallbackPattern(original_pattern, flags)

    cdef Pattern pypattern = Pattern()
    cdef map[cpp_string, int] named_groups = re_pattern.NamedCapturingGroups()
    pypattern.pattern = original_pattern
    pypattern.re_pattern = re_pattern
    pypattern.groups = re_pattern.NumberOfCapturingGroups()
    pypattern.encoded = encoded
    pypattern.flags = flags
    pypattern.groupindex = {}
    for it in named_groups:
        pypattern.groupindex[cpp_to_unicode(it.first)] = it.second

    if flags & DEBUG:
        print(repr(pypattern._dump_pattern()))
    del s
    return pypattern


def _prepare_pattern(bytes pattern, int flags):
    """Translate pattern to RE2 syntax."""
    cdef bytearray result = bytearray()
    cdef unsigned char * cstring = pattern
    cdef unsigned char this, that
    cdef int size = len(pattern)
    cdef int n = 0

    if flags & (_S | _M):
        result.extend(b'(?')
        if flags & _S:
            result.extend(b's')
        if flags & _M:
            result.extend(b'm')
        result.extend(b')')
    while n < size:
        this = cstring[n]
        if flags & _X:
            if this in b' \t\n\r\f\v':
                n += 1
                continue
            elif this == b'#':
                while True:
                    n += 1
                    if n >= size:
                        break
                    this = cstring[n]
                    if this == b'\n':
                        break
                n += 1
                continue

        if this != b'[' and this != b'\\':
            result.append(this)
            n += 1
            continue
        elif this == b'[':
            result.append(this)
            while True:
                n += 1
                if n >= size:
                    raise RegexError("unexpected end of regular expression")
                this = cstring[n]
                if this == b']':
                    result.append(this)
                    break
                elif this == b'\\':
                    n += 1
                    that = cstring[n]
                    if that == b'b':
                        result.extend(br'\010')
                    elif flags & _U:
                        if that == b'd':
                            result.extend(br'\p{Nd}')
                        elif that == b'w':
                            result.extend(br'_\p{L}\p{Nd}')
                        elif that == b's':
                            result.extend(br'\s\p{Z}')
                        elif that == b'D':
                            result.extend(br'\P{Nd}')
                        elif that == b'W':
                            # Since \w and \s are made out of several character
                            # groups, I don't see a way to convert their
                            # complements into a group without rewriting the
                            # whole expression, which seems too complicated.
                            raise CharClassProblemException()
                        elif that == b'S':
                            raise CharClassProblemException()
                        else:
                            result.append(this)
                            result.append(that)
                    else:
                        result.append(this)
                        result.append(that)
                else:
                    result.append(this)
        elif this == b'\\':
            n += 1
            that = cstring[n]
            if b'8' <= that <= b'9':
                raise BackreferencesException()
            elif isoct(that):
                if (n + 2 < size and isoct(cstring[n + 1])
                        and isoct(cstring[n + 2])):
                    # all clear, this is an octal escape
                    result.extend(cstring[n - 1:n + 3])
                    n += 2
                else:
                    raise BackreferencesException()
            elif that == b'x':
                if (n + 2 < size and ishex(cstring[n + 1])
                        and ishex(cstring[n + 2])):
                    # hex escape
                    result.extend(cstring[n - 1:n + 3])
                    n += 2
                else:
                    raise BackreferencesException()
            elif that == b'Z':
                result.extend(b'\\z')
            elif flags & _U:
                if that == b'd':
                    result.extend(br'\p{Nd}')
                elif that == b'w':
                    result.extend(br'[_\p{L}\p{Nd}]')
                elif that == b's':
                    result.extend(br'[\s\p{Z}]')
                elif that == b'D':
                    result.extend(br'[^\p{Nd}]')
                elif that == b'W':
                    result.extend(br'[^_\p{L}\p{Nd}]')
                elif that == b'S':
                    result.extend(br'[^\s\p{Z}]')
                else:
                    result.append(this)
                    result.append(that)
            else:
                result.append(this)
                result.append(that)
        n += 1
    return bytes(result)
