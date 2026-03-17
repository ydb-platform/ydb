import fnmatch
import functools
import re

#
# Globbing helpers
#


@functools.cache
def _is_case_sensitive(flavour):
    return flavour.normcase("Aa") == "Aa"


# fnmatch.translate() returns a regular expression that includes a prefix and
# a suffix, which enable matching newlines and ensure the end of the string is
# matched, respectively. These features are undesirable for our implementation
# of PurePatch.match(), which represents path separators as newlines and joins
# pattern segments together. As a workaround, we define a slice object that
# can remove the prefix and suffix from any translate() result. See the
# _compile_pattern_lines() function for more details.
_FNMATCH_PREFIX, _FNMATCH_SUFFIX = fnmatch.translate("_").split("_")
_FNMATCH_SLICE = slice(len(_FNMATCH_PREFIX), -len(_FNMATCH_SUFFIX))
_SWAP_SEP_AND_NEWLINE = {
    "/": str.maketrans({"/": "\n", "\n": "/"}),
    "\\": str.maketrans({"\\": "\n", "\n": "\\"}),
}


@functools.lru_cache()
def _make_selector(pattern_parts, flavour, case_sensitive):
    pat = pattern_parts[0]
    if not pat:
        return _TerminatingSelector()
    if pat == "**":
        child_parts_idx = 1
        while child_parts_idx < len(pattern_parts) and pattern_parts[child_parts_idx] == "**":
            child_parts_idx += 1
        child_parts = pattern_parts[child_parts_idx:]
        if "**" in child_parts:
            cls = _DoubleRecursiveWildcardSelector
        else:
            cls = _RecursiveWildcardSelector
    else:
        child_parts = pattern_parts[1:]
        if pat == "..":
            cls = _ParentSelector
        elif "**" in pat:
            raise ValueError("Invalid pattern: '**' can only be an entire path component")
        else:
            cls = _WildcardSelector
    return cls(pat, child_parts, flavour, case_sensitive)


@functools.lru_cache(maxsize=256)
def _compile_pattern(pat, case_sensitive):
    flags = re.NOFLAG if case_sensitive else re.IGNORECASE
    return re.compile(fnmatch.translate(pat), flags).match


@functools.lru_cache()
def _compile_pattern_lines(pattern_lines, case_sensitive):
    """Compile the given pattern lines to an `re.Pattern` object.

    The *pattern_lines* argument is a glob-style pattern (e.g. '*/*.py') with
    its path separators and newlines swapped (e.g. '*\n*.py`). By using
    newlines to separate path components, and not setting `re.DOTALL`, we
    ensure that the `*` wildcard cannot match path separators.

    The returned `re.Pattern` object may have its `match()` method called to
    match a complete pattern, or `search()` to match from the right. The
    argument supplied to these methods must also have its path separators and
    newlines swapped.
    """

    # Match the start of the path, or just after a path separator
    parts = ["^"]
    for part in pattern_lines.splitlines(keepends=True):
        if part == "*\n":
            part = r".+\n"
        elif part == "*":
            part = r".+"
        else:
            # Any other component: pass to fnmatch.translate(). We slice off
            # the common prefix and suffix added by translate() to ensure that
            # re.DOTALL is not set, and the end of the string not matched,
            # respectively. With DOTALL not set, '*' wildcards will not match
            # path separators, because the '.' characters in the pattern will
            # not match newlines.
            part = fnmatch.translate(part)[_FNMATCH_SLICE]
        parts.append(part)
    # Match the end of the path, always.
    parts.append(r"\Z")
    flags = re.MULTILINE
    if not case_sensitive:
        flags |= re.IGNORECASE
    return re.compile("".join(parts), flags=flags)


class _Selector:
    """A selector matches a specific glob pattern part against the children
    of a given path."""

    def __init__(self, child_parts, flavour, case_sensitive):
        self.child_parts = child_parts
        if child_parts:
            self.successor = _make_selector(child_parts, flavour, case_sensitive)
            self.dironly = True
        else:
            self.successor = _TerminatingSelector()
            self.dironly = False

    def select_from(self, parent_path):
        """Iterate over all child paths of `parent_path` matched by this
        selector.  This can contain parent_path itself."""
        path_cls = type(parent_path)
        scandir = path_cls._scandir
        if not parent_path.is_dir():
            return iter([])
        return self._select_from(parent_path, scandir)


class _TerminatingSelector:

    def _select_from(self, parent_path, scandir):
        yield parent_path


class _ParentSelector(_Selector):

    def __init__(self, name, child_parts, flavour, case_sensitive):
        _Selector.__init__(self, child_parts, flavour, case_sensitive)

    def _select_from(self, parent_path, scandir):
        path = parent_path._make_child_relpath("..")
        for p in self.successor._select_from(path, scandir):
            yield p


class _WildcardSelector(_Selector):

    def __init__(self, pat, child_parts, flavour, case_sensitive):
        _Selector.__init__(self, child_parts, flavour, case_sensitive)
        if case_sensitive is None:
            # TODO: evaluate case-sensitivity of each directory in _select_from()
            case_sensitive = _is_case_sensitive(flavour)
        self.match = _compile_pattern(pat, case_sensitive)

    def _select_from(self, parent_path, scandir):
        try:
            # We must close the scandir() object before proceeding to
            # avoid exhausting file descriptors when globbing deep trees.
            with scandir(parent_path) as scandir_it:
                entries = list(scandir_it)
        except OSError:
            pass
        else:
            for entry in entries:
                if self.dironly:
                    try:
                        if not entry.is_dir():
                            continue
                    except OSError:
                        continue
                name = entry.name
                if self.match(name):
                    path = parent_path._make_child_relpath(name)
                    for p in self.successor._select_from(path, scandir):
                        yield p


class _RecursiveWildcardSelector(_Selector):

    def __init__(self, pat, child_parts, flavour, case_sensitive):
        _Selector.__init__(self, child_parts, flavour, case_sensitive)

    def _iterate_directories(self, parent_path):
        yield parent_path
        for dirpath, dirnames, _ in parent_path.walk():
            for dirname in dirnames:
                yield dirpath._make_child_relpath(dirname)

    def _select_from(self, parent_path, scandir):
        successor_select = self.successor._select_from
        for starting_point in self._iterate_directories(parent_path):
            for p in successor_select(starting_point, scandir):
                yield p


class _DoubleRecursiveWildcardSelector(_RecursiveWildcardSelector):
    """
    Like _RecursiveWildcardSelector, but also de-duplicates results from
    successive selectors. This is necessary if the pattern contains
    multiple non-adjacent '**' segments.
    """

    def _select_from(self, parent_path, scandir):
        yielded = set()
        try:
            for p in super()._select_from(parent_path, scandir):
                if p not in yielded:
                    yield p
                    yielded.add(p)
        finally:
            yielded.clear()
