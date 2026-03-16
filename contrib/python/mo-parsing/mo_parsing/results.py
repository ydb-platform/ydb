# encoding: utf-8
import inspect

from mo_dots import is_many, is_null, register_data, register_list, exists
from mo_future import is_text, text, zip_longest, MutableMapping
from mo_imports import expect, export
from mo_parsing.exceptions import sort_causes

from mo_parsing.utils import Log, enlist

Suppress, ParserElement, NO_PARSER, NO_RESULTS, Group, Dict, Token, Empty = expect(
    "Suppress",
    "ParserElement",
    "NO_PARSER",
    "NO_RESULTS",
    "Group",
    "Dict",
    "Token",
    "Empty",
)


class ParseResults(object):
    __slots__ = ["_type", "start", "end", "tokens", "timing", "failures"]

    @property
    def name(self):
        return self._type.token_name

    @property
    def type(self):
        return self._type

    def __init__(self, result_type, start, end, tokens, failures):
        if end == -1:
            Log.error("not allowed")
        self._type = result_type
        self.start = start
        self.end = end
        self.tokens = tokens
        self.timing = None
        if len(failures) > 30:
            failures = sort_causes(failures)
            best_index = failures[0].start
            self.failures = [f for f in failures if f.start == best_index]
        else:
            self.failures = failures

    def _get_item_by_name(self, name):
        # return open list of values for given name
        for tok in self.tokens:
            if isinstance(tok, ParseResults):
                if tok.name == name:
                    if isinstance(tok.type, Group):
                        yield tok
                    else:
                        for t in tok.tokens:
                            yield from _flatten(t)
                    continue
                elif tok.name:
                    continue
                elif isinstance(tok.type, Group):
                    continue
                yield from tok._get_item_by_name(name)

    def __getitem__(self, item):
        if is_text(item):
            values = list(self._get_item_by_name(item))
            if len(values) == 0:
                return NO_RESULTS
            if len(values) == 1:
                return values[0]
            # ENCAPSULATE IN A ParseResults FOR FURTHER NAVIGATION
            return ParseResults(NO_PARSER, -1, 0, values, [])
        elif isinstance(item, int):
            if item < 0:
                item = len(self) + item
            for ii, v in enumerate(self):
                if item == ii:
                    return v
        elif isinstance(item, slice):
            return list(iter(self))[item]
        else:
            Log.error("not expected")

    def __setitem__(self, k, v):
        if isinstance(k, (slice, int)):
            Log.error("not supported")

        if v is None:
            v = NO_RESULTS

        for i, tok in enumerate(self.tokens):
            if isinstance(tok, ParseResults):
                if tok.name == k:
                    self.tokens[i] = v
                    v = NO_RESULTS  # ERASE ALL OTHERS
                elif isinstance(tok.type, Group):
                    continue
                elif tok.name:
                    continue

                tok.__setitem__(k, NO_RESULTS)  # ERASE ALL CHILDREN

        if v is not NO_RESULTS:
            tokens = self.tokens
            if isinstance(self, ForwardResults):
                tokens = tokens[0].tokens
            if isinstance(v, ParseResults):
                tokens.append(Annotation(k, v.start, v.end, v.tokens))
            else:
                tokens.append(Annotation(k, -1, 0, enlist(v)))

    def __contains__(self, k):
        return any((r.name) == k for r in self.tokens)

    def length(self):
        return sum(1 for _ in self)

    def __eq__(self, other):
        if is_null(other):
            return not self.__bool__()
        elif is_text(other):
            try:
                return "".join(self) == other
            except Exception as e:
                return False
        elif is_many(other):
            return all(s == o for s, o in zip_longest(self, other))
        elif self.length() == 1:
            return self[0] == other
        elif not self:
            return False
        elif isinstance(other, dict):
            for k, v in other.items():
                if self[k] != v:
                    return False
            return True
        else:
            Log.error("do not know how to handle")

    def __bool__(self):
        if not self.tokens:
            return False

        for r in self.tokens:
            if not isinstance(r, ParseResults):
                return True
            elif r.name:
                return True
            elif isinstance(r.type, Group):
                return True
            else:
                if r.__bool__():
                    return True

        return False

    __nonzero__ = __bool__

    def __len__(self):
        return sum(1 for _ in self)

    def __iter__(self):
        for r in self.tokens:
            if isinstance(r, Annotation):
                continue
            elif not isinstance(r, ParseResults):
                yield r
            elif isinstance(r.type, Group):
                yield r
            else:
                yield from r

    def __delitem__(self, key):
        if isinstance(key, (int, slice)):
            Log.error("not allowed")
        else:
            self[key] = NO_RESULTS

    def __reversed__(self):
        return reversed(self.tokens)

    def value(self):
        """
        RETURN WHATEVER PRIMITIVE VALUES ARE LEFT
        """
        value = [v.value() if isinstance(v, ParseResults) else v for v in self]
        if not value:
            return None
        elif len(value) == 1:
            return value[0]
        else:
            return value

    def keys(self):
        for k, _ in self.items():
            yield k

    def values(self):
        for _, v in self.items():
            yield v

    def items(self):
        output = {}
        for tok in self.tokens:
            if isinstance(tok, ParseResults):
                if isinstance(tok.type, Suppress):
                    continue
                if tok.name:
                    add(output, tok.name, [tok])
                    continue
                if isinstance(tok.type, Group):
                    continue
                for k, v in tok.items():
                    add(output, k, v)
        yield from output.items()

    def get(self, key, default_value=None):
        """
        Returns named result matching the given key, or if there is no
        such name, then returns the given ``default_value``

        Similar to ``dict.get()``.
        """
        if key in self:
            return self[key]
        else:
            return default_value

    def __contains__(self, item):
        if item is Ellipsis:
            return False
        return exists(self[item])

    def __add__(self, other):
        # if not isinstance(other, ParseResults):
        #     return self.value() + other
        #
        return ParseResults(
            Group(self.type + other.type),
            self.start,
            other.end,
            self.tokens + other.tokens,
            self.failures + other.failures,
        )

    def __radd__(self, other):
        if not other:  # happens when using sum() on parsers
            return self
        other = whitespaces.CURRENT.normalize(other)
        return other + self

    def __repr__(self):
        try:
            return repr(self.tokens)
        except Exception as e:
            Log.warning("problem", cause=e)
            return "[]"

    def __data__(self):
        return [
            v.__data__() if isinstance(v, ParserElement) else v for v in self.tokens
        ]

    def __str__(self):
        if len(inspect.stack(0)) > 30:
            return "..."
        elif not self.tokens:
            return ""
        elif len(self.tokens) == 1:
            return text(self.tokens[0])
        else:
            return "[" + ", ".join(text(v) for v in self.tokens) + "]"

    def _asStringList(self):
        for t in self:
            if isinstance(t, ParseResults):
                for s in t._asStringList():
                    yield s
            else:
                yield t

    def as_string(self, sep=""):
        return sep.join(self._asStringList())

    def as_list(self):
        """
        Returns the parse results as a nested list of matching tokens, all converted to strings.

        Example::

            patt = OneOrMore(Word(alphas))
            result = patt.parse_string("sldkj lsdkj sldkj")
            # even though the result prints in string-like form, it is actually a mo_parsing ParseResults
            print(type(result), result) # -> <class 'mo_parsing.ParseResults'> ['sldkj', 'lsdkj', 'sldkj']

            # Use as_list() to create an actual list
            result_list = result.as_list()
            print(type(result_list), result_list) # -> <class 'list'> ['sldkj', 'lsdkj', 'sldkj']
        """

        def internal(obj, depth):
            # RETURN AN OPEN LIST
            if depth > 60:
                Log.warning("deep!")

            if isinstance(obj, Annotation):
                return []
            elif isinstance(obj, ParseResults):
                output = []
                for t in obj.tokens:
                    inner = internal(t, depth + 1)
                    output.extend(inner)
                if isinstance(obj.type, Group):
                    return [output]
                else:
                    return output
            else:
                return [obj]

        output = internal(self, 0)
        # if isinstance(self.type, Group):
        #     return simpler(output)
        # else:
        return output

    def __copy__(self):
        """
        Returns a new copy of a `ParseResults` object.
        """
        ret = ParseResults(
            self.type, self.start, self.end, list(self.tokens), self.failures
        )
        return ret

    def get_name(self):
        r"""
        Returns the results name for this token expression. Useful when several
        different expressions might match at a particular location.
        """
        if self.name:
            return self.name
        elif len(self.tokens) == 1:
            return self.tokens[0].name
        else:
            return ""


class ForwardResults(ParseResults):
    __slots__ = []

    @property
    def type(self):
        return self.tokens[0].type

    def __getitem__(self, item):
        return self.tokens[0][item]

    def __setitem__(self, k, v):
        self.tokens[0][k] = v

    def __bool__(self):
        return self.tokens[0].__bool__()

    def __iter__(self):
        if len(self.tokens) != 1:
            Log.error("not expected")

        yield from self.tokens[0]

    def items(self):
        yield from self.tokens[0].items()


def _flatten(token):
    """
    FLATTEN SOME, LEAVING ANY IMPORTANT FEATURES (names or groups)
    """
    if not isinstance(token, ParseResults):
        yield token
    elif isinstance(token.type, Group):
        yield token
    elif token.name:
        yield token
    else:
        for t in token.tokens:
            for tt in _flatten(t):
                yield tt


def add(obj, key, value):
    if not isinstance(value, list):
        Log.error("not allowed")
    if value and isinstance(value[0], list):
        Log.error("not expected")
    old_v = obj.get(key)
    if old_v is None:
        obj[key] = value
    else:
        old_v.extend(value)


class Annotation(ParseResults):
    # Append one of these to the parse results to
    # add key: value pair not found in the original text

    __slots__ = []

    def __init__(self, name, start, end, value):
        if not name:
            Log.error("expecting a name")
        if not isinstance(value, list):
            Log.error("expecting a list")
        ParseResults.__init__(self, Empty()(name), start, end, value, [])

    def __str__(self):
        return "{" + text(self.name) + ": " + text(self.tokens) + "}"

    def __repr__(self):
        return "Annotation(" + repr(self.name) + ", " + repr(self.tokens) + ")"


MutableMapping.register(ParseResults)
register_data(ParseResults)
register_list(ParseResults)

from mo_parsing import whitespaces
export("mo_parsing.utils", ParseResults)
