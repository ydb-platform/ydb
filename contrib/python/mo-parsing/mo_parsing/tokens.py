# encoding: utf-8

from mo_future import is_text
from mo_imports import export

from mo_parsing import whitespaces
from mo_parsing.core import ParserElement
from mo_parsing.exceptions import ParseException
from mo_parsing.results import ParseResults
from mo_parsing.utils import *
from mo_parsing.whitespaces import Whitespace

Regex = expect("Regex")


class Token(ParserElement):
    """
    Represent some contiguous set for characters, no whitespace
    """

    __slots__ = []
    Config = append_config(ParserElement, "match", "regex")

    def __init__(self):
        ParserElement.__init__(self)
        self.streamlined = True


class Empty(Token):
    """
    An empty token, will always match.
    Often used to consume-and-suppress trailing whitespace
    """

    zero_length = True
    __slots__ = []

    def __init__(self, name=None):
        Token.__init__(self)
        self.parser_name = name

    def is_annotated(self):
        return self.parse_action or self.token_name

    def min_length(self):
        return 0

    def parse_impl(self, string, start, do_actions=True):
        end = start
        # end = self.whitespace.skip(string, start)
        return ParseResults(self, start, end, [], [])

    def streamline(self):
        return self

    def reverse(self):
        return self

    def __regex__(self):
        return self.whitespace.__regex__()

    def __str__(self):
        return self.parser_name or "Empty"


class NoMatch(Token):
    """A token that will never match."""

    zero_length = True
    __slots__ = []

    def __init__(self):
        super(NoMatch, self).__init__()
        self.parser_name = "NoMatch"

    def parse_impl(self, string, loc, do_actions=True):
        raise ParseException(self, loc, string)

    def min_length(self):
        return 0

    def reverse(self):
        return self

    def __regex__(self):
        return "+", "a^"


class AnyChar(Token):
    __slots__ = []

    def __init__(self):
        """
        Match any single character
        """
        Token.__init__(self)
        self.parser_name = "AnyChar"

    def parse_impl(self, string, loc, do_actions=True):
        if loc >= len(string):
            raise ParseException(self, loc, string)
        return ParseResults(self, loc, loc + 1, [string[loc]], [])

    def min_length(self):
        return 1

    def reverse(self):
        return self

    def __regex__(self):
        return "*", "."


class Literal(Token):
    """Token to exactly match a specified string."""

    __slots__ = []

    def __init__(self, match):
        if not is_text(match):
            Log.error("Expecting string for literal")
        Token.__init__(self)

        self.set_config(match=match, regex=regex_compile(re.escape(match)))

        if len(match) == 0:
            Log.error("Literal must be at least one character")
        elif len(match) == 1:
            self.__class__ = SingleCharLiteral

    def parse_impl(self, string, start, do_actions=True):
        match = self.parser_config.match
        if string.startswith(match, start):
            end = start + len(match)
            return ParseResults(self, start, end, [match], [])
        raise ParseException(self, start, string)

    def expecting(self):
        return {self.parser_config.match.lower(): [self]}

    def _min_length(self):
        return len(self.parser_config.match)

    def reverse(self):
        return Literal(self.parser_config.match[::-1])

    def __regex__(self):
        return "+", self.parser_config.regex.pattern

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return self.parser_config.match


class SingleCharLiteral(Literal):
    __slots__ = []

    def parse_impl(self, string, start, do_actions=True):
        try:
            if string[start] == self.parser_config.match:
                return ParseResults(
                    self, start, start + 1, [self.parser_config.match], []
                )
        except IndexError:
            pass

        raise ParseException(self, start, string)

    def min_length(self):
        return 1

    def reverse(self):
        return self


class Keyword(Token):
    __slots__ = []
    Config = append_config(Token, "ident_chars")

    def __init__(
        self,
        match,
        ident_chars = None,  # required to identify word boundary
        caseless=None,
    ):
        Token.__init__(self)
        if ident_chars is None:
            ident_chars = whitespaces.CURRENT.keyword_chars
        elif isinstance(ident_chars, Regex):
            ident_chars = ident_chars.expr.parser_config.include

        if caseless:
            ident_chars = ident_chars.lower() + ident_chars.upper()

        ident_chars = "".join(sorted(set(ident_chars)))

        if caseless:
            pattern = regex_caseless(match)
        else:
            pattern = re.escape(match)

        non_word = "($|(?!" + regex_range(ident_chars) + "))"
        self.set_config(
            ident_chars=ident_chars,
            match=match,
            regex=regex_compile(pattern + non_word),
        )

        self.parser_name = match
        if caseless:
            self.__class__ = CaselessKeyword

    def parse_impl(self, string, start, do_actions=True):
        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(
                self, start, found.end(), [self.parser_config.match], []
            )
        raise ParseException(self, start, string)

    def expecting(self):
        return {self.parser_config.match.lower(): [self]}

    def _min_length(self):
        return len(self.parser_config.match)

    def reverse(self):
        return Keyword(self.parser_config.match[::-1], self.parser_config.ident_chars)

    def __regex__(self):
        return "+", self.parser_config.regex.pattern


class CaselessKeyword(Keyword):
    __slots__ = []

    def __init__(self, match, ident_chars=None):
        Keyword.__init__(self, match, ident_chars, caseless=True)

    def reverse(self):
        return Keyword(
            self.parser_config.match[::-1],
            self.parser_config.ident_chars,
            caseless=True,
        )


class CaselessLiteral(Literal):
    """
    Token to match a specified string, ignoring case of letters.
    Note: the matched results will always be in the case of the given
    match string, NOT the case of the input text.
    """

    __slots__ = []

    def __init__(self, match):
        if not is_text(match):
            Log.error("Expecting string for literal")
        if len(match) == 0:
            Log.error("Literal must be at least one character")
        Token.__init__(self)
        self.set_config(
            match=match, regex=regex_compile(regex_caseless(re.escape(match))),
        )
        self.parser_name = match

    def parse_impl(self, string, start, do_actions=True):
        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(
                self, start, found.end(), [self.parser_config.match], []
            )
        raise ParseException(self, start, string)

    def reverse(self):
        return CaselessLiteral(self.parser_config.match[::-1])


class CloseMatch(Token):
    """
    A variation on `Literal` which matches "close" matches,
    that is, strings with at most 'n' mismatching characters.
    `CloseMatch` takes parameters:

     - ``match_string`` - string to be matched
     - ``max_mismatches`` - (``default=1``) maximum number of
       mismatches allowed to count as a match

    The results from a successful parse will contain the matched text
    from the input string and the following named results:

     - ``mismatches`` - a list of the positions within the
       match_string where mismatches were found
     - ``original`` - the original match_string used to compare
       against the input string

    If ``mismatches`` is an empty list, then the match was an exact
    match.
    """

    __slots__ = []
    Config = append_config(Token, "max_mismatches")

    def __init__(self, match, max_mismatches=1):
        super(CloseMatch, self).__init__()
        self.parser_name = match
        self.set_config(match=match, max_mismatches=max_mismatches)

    def parse_impl(self, string, start, do_actions=True):
        end = start
        instrlen = len(string)
        maxloc = start + len(self.parser_config.match)

        if maxloc <= instrlen:
            match = self.parser_config.match
            match_stringloc = 0
            mismatches = []
            max_mismatches = self.parser_config.max_mismatches

            for match_stringloc, (src, mat) in enumerate(zip(
                string[end:maxloc], match
            )):
                if src != mat:
                    mismatches.append(match_stringloc)
                    if len(mismatches) > max_mismatches:
                        break
            else:
                end = match_stringloc + 1
                results = ParseResults(self, start, end, [string[start:end]], [])
                results["original"] = match
                results["mismatches"] = mismatches
                return results

        raise ParseException(self, start, string)

    def reverse(self):
        return CloseMatch(self.parser_config.match[
            ::-1, self.parser_config.max_mismatches
        ])


class Word(Token):
    __slots__ = ["regex"]
    Config = append_config(Token, "min", "init_chars")

    def __init__(
        self,
        init_chars,
        body_chars=None,
        min=1,
        max=None,
        exact=0,
        as_keyword=False,  # IF WE EXPECT NON-WORD CHARACTERS BEFORE AND AFTER
        exclude="",
    ):
        Token.__init__(self)

        # TODO: REPLACE body_chars AND init_chars WITH ANY PATTERN?
        if body_chars is None:
            body_chars = init_chars
        if exact:
            min = max = exact

        if min < 1:
            raise ValueError(
                "cannot specify a minimum length < 1; use Optional(Word()) if"
                " zero-length word is permitted"
            )

        if body_chars == init_chars:
            if init_chars.__class__.__name__ == "Regex":
                init_chars = init_chars.expecting().keys()
            prec, regexp = Char(init_chars, exclude=exclude)[min:max].__regex__()
        elif max is None or max == MAX_INT:
            with whitespaces.NO_WHITESPACE:
                prec, regexp = (
                    Char(init_chars, exclude=exclude)
                    + Char(body_chars, exclude=exclude)[min - 1 :]
                ).__regex__()
        else:
            with whitespaces.NO_WHITESPACE:
                prec, regexp = (
                    Char(init_chars, exclude=exclude)
                    + Char(body_chars, exclude=exclude)[min - 1 : max - 1]
                ).__regex__()

        if as_keyword:
            regexp = r"\b" + regexp + r"\b"

        self.regex = regex_compile(regexp)
        self.set_config(min=min, init_chars=init_chars)

    def copy(self):
        output = Token.copy(self)
        output.regex = self.regex
        return output

    def parse_impl(self, string, start, do_actions=True):
        found = self.regex.match(string, start)
        if found:
            return ParseResults(self, start, found.end(), [found.group()], [])
        else:
            raise ParseException(self, start, string)

    def min_length(self):
        return self.parser_config.min

    def expecting(self):
        if len(self.parser_config.init_chars) < 11:
            return {c: [self] for c in self.parser_config.init_chars}
        else:
            return {}

    def reverse(self):
        if self.parser_config.init_char == self.parser_config.body_chars:
            return self
        raise NotImplementedError()

    def __regex__(self):
        return "+", self.regex.pattern

    def __str__(self):
        if self.parser_name:
            return self.parser_name
        return f"W:({self.regex.pattern})"


class Char(Token):
    __slots__ = []
    Config = append_config(Token, "include", "exclude")

    def __init__(self, include="", as_keyword=False, exclude=""):
        """
        Represent one character in a given include
        """
        Token.__init__(self)
        include = set(include) if include else set()
        exclude = set(exclude) if exclude else set()
        include = "".join(sorted(include - exclude))
        exclude = "".join(sorted(exclude))

        if not include:
            regex = regex_range(exclude, exclude=True)
        else:
            regex = regex_range(include)

        if as_keyword:
            regex = r"\b%s\b" % self
        self.set_config(regex=regex_compile(regex), include=include, exclude=exclude)

    def parse_impl(self, string, start, do_actions=True):
        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(self, start, found.end(), [found.group()], [])

        raise ParseException(self, start, string)

    def expecting(self):
        return {c: [self] for c in self.parser_config.include}

    def min_length(self):
        return 1

    def reverse(self):
        return self

    def __regex__(self):
        return "*", self.parser_config.regex.pattern

    def __str__(self):
        return self.parser_config.regex.pattern


class CharsNotIn(Token):
    """
    Token for matching words composed of characters *not* in a given
    set (will include whitespace in matched characters if not listed in
    the provided exclusion set - see example). Defined with string
    containing all disallowed characters, and an optional minimum,
    maximum, and/or exact length.  The default value for ``min`` is
    1 (a minimum value < 1 is not valid); the default values for
    ``max`` and ``exact`` are 0, meaning no maximum or exact
    length restriction.
    """

    __slots__ = []
    Config = append_config(Token, "min_len", "max_len", "not_chars")

    def __init__(self, not_chars, min=1, max=0, exact=0):
        Token.__init__(self)
        not_chars = "".join(sorted(set(not_chars)))

        if min < 1:
            raise ValueError(
                "cannot specify a minimum length < 1; use "
                "Optional(CharsNotIn()) if zero-length char group is permitted"
            )

        max = max if max > 0 else MAX_INT
        if exact:
            min = exact
            max = exact

        if len(not_chars) == 1:
            regex = "[^" + regex_range(not_chars) + "]"
        else:
            regex = "[^" + regex_range(not_chars)[1:]

        if not max or max == MAX_INT:
            if min == 0:
                suffix = "*"
            elif min == 1:
                suffix = "+"
            else:
                suffix = "{" + str(min) + ":}"
        elif min == 1 and max == 1:
            suffix = ""
        else:
            suffix = "{" + str(min) + ":" + str(max) + "}"

        self.set_config(
            regex=regex_compile(regex + suffix),
            min_len=min,
            max_len=max,
            not_chars=not_chars,
        )
        self.parser_name = text(self)

    def parse_impl(self, string, start, do_actions=True):
        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(self, start, found.end(), [found.group()], [])

        raise ParseException(self, start, string)

    def min_length(self):
        return self.parser_config.min_len

    def reverse(self):
        return self

    def __regex__(self):
        return "*", self.parser_config.regex.pattern

    def __str__(self):
        return self.parser_config.regex.pattern


class White(Token):
    """
    Special matching class for matching whitespace.  Normally,
    whitespace is ignored by mo_parsing grammars.  This class is included
    when some whitespace structures are significant.  Define with
    a string containing the whitespace characters to be matched; default
    is ``" \\t\\r\\n"``.  Also takes optional ``min``,
    ``max``, and ``exact`` arguments, as defined for the
    `Word` class.
    """

    white_strs = {
        " ": "<SP>",
        "\t": "<TAB>",
        "\n": "<LF>",
        "\r": "<CR>",
        "\f": "<FF>",
        "\u00A0": "<NBSP>",
        "\u1680": "<OGHAM_SPACE_MARK>",
        "\u180E": "<MONGOLIAN_VOWEL_SEPARATOR>",
        "\u2000": "<EN_QUAD>",
        "\u2001": "<EM_QUAD>",
        "\u2002": "<EN_SPACE>",
        "\u2003": "<EM_SPACE>",
        "\u2004": "<THREE-PER-EM_SPACE>",
        "\u2005": "<FOUR-PER-EM_SPACE>",
        "\u2006": "<SIX-PER-EM_SPACE>",
        "\u2007": "<FIGURE_SPACE>",
        "\u2008": "<PUNCTUATION_SPACE>",
        "\u2009": "<THIN_SPACE>",
        "\u200A": "<HAIR_SPACE>",
        "\u200B": "<ZERO_WIDTH_SPACE>",
        "\u202F": "<NNBSP>",
        "\u205F": "<MMSP>",
        "\u3000": "<IDEOGRAPHIC_SPACE>",
    }

    __slots__ = []
    Config = append_config(Token, "min_len", "max_len", "white_chars")

    def __init__(self, ws=" \t\r\n", min=1, max=0, exact=0):
        Token.__init__(self)
        white_chars = "".join(sorted(set(ws)))
        self.parser_name = "|".join(White.white_strs[c] for c in white_chars)

        max = max if max > 0 else MAX_INT
        if exact > 0:
            max = exact
            min = exact
        self.set_config(min_len=min, max_len=max, white_chars=white_chars)

    def parse_impl(self, string, start, do_actions=True):
        if string[start] not in self.parser_config.white_chars:
            raise ParseException(self, start, string)
        end = start
        end += 1
        maxloc = start + self.parser_config.max_len
        maxloc = min(maxloc, len(string))
        while end < maxloc and string[end] in self.parser_config.white_chars:
            end += 1

        if end - start < self.parser_config.min_len:
            raise ParseException(self, end, string)

        return ParseResults(
            self, start, end, string[start:end], [ParseException(self, end, string)]
        )

    def reverse(self):
        return self


class LineStart(Token):
    """
    Matches if current position is at the beginning of a line within
    the parse string
    """

    zero_length = True
    __slots__ = []

    def __init__(self):
        Token.__init__(self)
        self.parser_name = self.__class__.__name__

    def parse_impl(self, string, start, do_actions=True):
        if col(start, string) == 1:
            return ParseResults(self, start, start, [], [])
        raise ParseException(self, start, string)

    def min_length(self):
        return 0

    def reverse(self):
        return LineEnd()

    def __regex__(self):
        return "*", "^"


class LineEnd(Token):
    """
    Matches if current position is at the end of a line
    """

    zero_length = True
    __slots__ = []

    # Config = append_config(Token, "regex")

    def __init__(self):
        with Whitespace(" \t"):
            Token.__init__(self)
            self.parser_name = self.__class__.__name__
            self.set_config(regex=regex_compile("\\r?(\\n|$)"))

    def parse_impl(self, string, start, do_actions=True):
        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(self, start, found.end(), ["\n"], [])
        raise ParseException(self, start, string)

    def min_length(self):
        return 0

    def reverse(self):
        return LineStart()

    def __regex__(self):
        return "|", self.parser_config.regex.pattern


class StringStart(Token):
    """
    Matches if current position is at the beginning of the parse string
    """

    zero_length = True
    __slots__ = []

    def __init__(self):
        Token.__init__(self)
        self.parser_name = self.__class__.__name__

    def parse_impl(self, string, loc, do_actions=True):
        if loc != 0:
            # see if entire string up to here is just whitespace and ignoreables
            # if loc != self.whitespace.skip(string, 0):
            raise ParseException(self, loc, string)
        return []

    def min_length(self):
        return 0

    def reverse(self):
        return StringEnd()


class StringEnd(Token):
    """
    Matches if current position is at the end of the parse string
    """

    zero_length = True
    __slots__ = []

    def __init__(self):
        Token.__init__(self)
        self.parser_name = self.__class__.__name__
        self.set_config(regex=regex_compile("(?:\\r?\\n)*$"))

    def parse_impl(self, string, start, do_actions=True):
        end = len(string)
        if start >= end:
            return ParseResults(self, end, end, [], [])

        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(self, start, found.end(), [], [])
        raise ParseException(self, start, string)

    def min_length(self):
        return 0

    def reverse(self):
        return StringStart()


class WordStart(Token):
    """
    Matches if the current position is at the beginning of a Word,
    and is not preceded by any character in a given set of
    ``word_chars`` (default= ``printables``). To emulate the
    ``\b`` behavior of regular expressions, use
    ``WordStart(alphanums)``. ``WordStart`` will also match at
    the beginning of the string being parsed, or at the beginning of
    a line.
    """

    zero_length = True
    __slots__ = []
    Config = append_config(Token, "word_chars")

    def __init__(self, word_chars=printables):
        Token.__init__(self)
        self.parser_name = self.__class__.__name__
        self.set_config(
            regex=regex_compile(
                f"(?:(?<={(CharsNotIn(word_chars, exact=1)).__regex__()[1]})|^)(?={Char(word_chars).__regex__()[1]})"
            ),
            word_chars="".join(sorted(set(word_chars))),
        )
        self.streamlined = True

    def parse_impl(self, string, start, do_actions=True):
        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(self, start, start, [], [])
        raise ParseException(self, start, string)

    def min_length(self):
        return 0

    def reverse(self):
        return WordEnd(self.parser_config.word_chars)

    def __regex__(self):
        return "+", self.parser_config.regex.pattern


class WordEnd(Token):
    """
    Matches if the current position is at the end of a Word, and is
    not followed by any character in a given set of ``word_chars``
    (default= ``printables``). To emulate the ``\b`` behavior of
    regular expressions, use ``WordEnd(alphanums)``. ``WordEnd``
    will also match at the end of the string being parsed, or at the end
    of a line.
    """

    zero_length = True
    __slots__ = []
    Config = append_config(Token, "word_chars")

    def __init__(self, word_chars=None):
        Token.__init__(self)
        word_chars = coalesce(word_chars, whitespaces.CURRENT.keyword_chars)
        self.parser_name = self.__class__.__name__
        self.set_config(
            word_chars="".join(sorted(set(word_chars))),
            regex=regex_compile(
                f"(?<={Char(word_chars).__regex__()[1]})({(~Char(word_chars)).__regex__()[1]}|$)"
            ),
        )

    def copy(self):
        output = Token.copy(self)
        return output

    def parse_impl(self, string, start, do_actions=True):
        found = self.parser_config.regex.match(string, start)
        if found:
            return ParseResults(self, start, start, [], [])
        else:
            raise ParseException(self, start, string)

    def min_length(self):
        return 0

    def reverse(self):
        return WordStart(word_chars=self.parser_config.word_chars)

    def __regex__(self):
        return "+", self.parser_config.regex.pattern


export("mo_parsing.results", Token)
export("mo_parsing.results", Empty)

export("mo_parsing.core", Empty)
export("mo_parsing.core", StringEnd)
export("mo_parsing.core", Literal)
export("mo_parsing.core", Token)

export("mo_parsing.whitespaces", Literal)
export("mo_parsing.whitespaces", Token)

export("mo_parsing.enhancement", Token)
export("mo_parsing.enhancement", NoMatch)
export("mo_parsing.enhancement", Literal)
export("mo_parsing.enhancement", Keyword)
export("mo_parsing.enhancement", Word)
export("mo_parsing.enhancement", CharsNotIn)
export("mo_parsing.enhancement", StringEnd)
export("mo_parsing.enhancement", Empty)
export("mo_parsing.enhancement", Char)
