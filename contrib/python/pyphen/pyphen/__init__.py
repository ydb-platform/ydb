"""

Pyphen
======

Pure Python module to hyphenate text, inspired by Ruby's Text::Hyphen.

"""

import re
from importlib import resources
from pathlib import Path

VERSION = __version__ = '0.17.2'

__all__ = ('LANGUAGES', 'Pyphen', 'language_fallback')

# cache of per-file HyphDict objects
hdcache = {}

# precompile some stuff
parse_hex = re.compile(r'\^{2}([0-9a-f]{2})').sub
parse = re.compile(r'(\d?)(\D?)').findall
ignored = (
    '%', '#', 'LEFTHYPHENMIN', 'RIGHTHYPHENMIN',
    'COMPOUNDLEFTHYPHENMIN', 'COMPOUNDRIGHTHYPHENMIN')

#: Dict of languages including codes as keys and dictionary Path as values.
LANGUAGES = {}

try:
    dictionaries = resources.files('pyphen.dictionaries')
except TypeError:
    dictionaries = Path(__file__).parent / 'dictionaries'

for path in sorted(dictionaries.iterdir()):
    if path.suffix == '.dic':
        name = path.name[5:-4]
        LANGUAGES[name] = path
        short_name = name.split('_')[0]
        if short_name not in LANGUAGES:
            LANGUAGES[short_name] = path

LANGUAGES_LOWERCASE = {name.lower(): name for name in LANGUAGES}


def language_fallback(language):
    """Get a fallback language available in our dictionaries.

    http://www.unicode.org/reports/tr35/#Locale_Inheritance

    We use the normal truncation inheritance. This function needs aliases
    including scripts for languages with multiple regions available.

    """
    parts = language.replace('-', '_').lower().split('_')
    while parts:
        language = '_'.join(parts)
        if language in LANGUAGES_LOWERCASE:
            return LANGUAGES_LOWERCASE[language]
        parts.pop()


class AlternativeParser:
    """Parser of nonstandard hyphen pattern alternative.

    The instance returns a special int with data about the current position in
    the pattern when called with an odd value.

    """
    def __init__(self, pattern, alternative):
        alternative = alternative.split(',')
        self.change = alternative[0]
        self.index = int(alternative[1])
        self.cut = int(alternative[2])
        if pattern.startswith('.'):
            self.index += 1

    def __call__(self, value):
        self.index -= 1
        value = int(value)
        if value & 1:
            return DataInt(value, (self.change, self.index, self.cut))
        else:
            return value


class DataInt(int):
    """``int`` with some other data can be stuck to in a ``data`` attribute."""
    def __new__(cls, value, data=None, reference=None):
        """Create a new ``DataInt``.

        Call with ``reference=dataint_object`` to use the data from another
        ``DataInt``.

        """
        obj = int.__new__(cls, value)
        if reference and isinstance(reference, DataInt):
            obj.data = reference.data
        else:
            obj.data = data
        return obj


class HyphDict:
    """Hyphenation patterns."""

    def __init__(self, path):
        """Read a ``hyph_*.dic`` and parse its patterns.

        :param path: Path of hyph_*.dic to read

        """
        self.patterns = {}

        # see "man 4 hunspell", iscii-devanagari is not supported by python
        with path.open('rb') as fd:
            encoding = fd.readline().decode()
        if encoding.lower() == 'microsoft-cp1251':
            encoding = 'cp1251'

        for pattern in path.read_text(encoding).split('\n')[1:]:
            pattern = pattern.strip()
            if not pattern or pattern.startswith(ignored):
                continue

            # replace ^^hh with the real character
            pattern = parse_hex(
                lambda match: chr(int(match.group(1), 16)), pattern)

            # read nonstandard hyphen alternatives
            if '/' in pattern and '=' in pattern:
                pattern, alternative = pattern.split('/', 1)
                factory = AlternativeParser(pattern, alternative)
            else:
                factory = int

            tags, values = zip(*[
                (string, factory(i or '0')) for i, string in parse(pattern)])

            # if only zeros, skip this pattern
            if max(values) == 0:
                continue

            # chop zeros from beginning and end, and store start offset
            start, end = 0, len(values)
            while not values[start]:
                start += 1
            while not values[end - 1]:
                end -= 1

            self.patterns[''.join(tags)] = start, values[start:end]

        self.cache = {}
        self.maxlen = max(len(key) for key in self.patterns)

    def positions(self, word):
        """Get a list of positions where the word can be hyphenated.

        :param word: unicode string of the word to hyphenate

        E.g. for the dutch word 'lettergrepen' this method returns ``[3, 6,
        9]``.

        Each position is a ``DataInt`` with a data attribute.

        If the data attribute is not ``None``, it contains a tuple with
        information about nonstandard hyphenation at that point: ``(change,
        index, cut)``.

        change
          a string like ``'ff=f'``, that describes how hyphenation should
          take place.

        index
          where to substitute the change, counting from the current point

        cut
          how many characters to remove while substituting the nonstandard
          hyphenation

        """
        word = word.lower()
        points = self.cache.get(word)
        if points is None:
            pointed_word = f'.{word}.'
            references = [0] * (len(pointed_word) + 1)

            for i in range(len(pointed_word) - 1):
                stop = min(i + self.maxlen, len(pointed_word)) + 1
                for j in range(i + 1, stop):
                    pattern = self.patterns.get(pointed_word[i:j])
                    if not pattern:
                        continue
                    offset, values = pattern
                    slice_ = slice(i + offset, i + offset + len(values))
                    references[slice_] = map(max, values, references[slice_])

            self.cache[word] = points = [
                DataInt(i - 1, reference=reference)
                for i, reference in enumerate(references) if reference % 2]
        return points


class Pyphen:
    """Hyphenation class, with methods to hyphenate strings in various ways."""

    def __init__(self, filename=None, lang=None, left=2, right=2, cache=True):
        """Create an hyphenation instance for given lang or filename.

        :param filename: filename or Path of hyph_*.dic to read
        :param lang: lang of the included dict to use if no filename is given
        :param left: minimum number of characters of the first syllabe
        :param right: minimum number of characters of the last syllabe
        :param cache: if ``True``, use cached copy of the hyphenation patterns

        """
        self.left, self.right = left, right
        if filename and not isinstance(filename, (str, Path)):
            path = filename
        else:
            path = Path(filename) if filename else LANGUAGES[language_fallback(lang)]
        if not cache or path not in hdcache:
            hdcache[path] = HyphDict(path)
        self.hd = hdcache[path]

    def positions(self, word):
        """Get a list of positions where the word can be hyphenated.

        :param word: unicode string of the word to hyphenate

        See also ``HyphDict.positions``. The points that are too far to the
        left or right are removed.

        """
        right = len(word) - self.right
        return [i for i in self.hd.positions(word) if self.left <= i <= right]

    def iterate(self, word):
        """Iterate over all hyphenation possibilities, the longest first.

        :param word: unicode string of the word to hyphenate

        """
        for position in reversed(self.positions(word)):
            if position.data:
                # get the nonstandard hyphenation data
                change, index, cut = position.data
                index += position
                if word.isupper():
                    change = change.upper()
                c1, c2 = change.split('=')
                yield word[:index] + c1, c2 + word[index + cut:]
            else:
                yield word[:position], word[position:]

    def wrap(self, word, width, hyphen='-'):
        """Get the longest possible first part and the last part of a word.

        :param word: unicode string of the word to hyphenate
        :param width: maximum length of the first part
        :param hyphen: unicode string used as hyphen character

        The first part has the hyphen already attached.

        Returns ``None`` if there is no hyphenation point before ``width``, or
        if the word could not be hyphenated.

        """
        width -= len(hyphen)
        for w1, w2 in self.iterate(word):
            if len(w1) <= width:
                return w1 + hyphen, w2

    def inserted(self, word, hyphen='-'):
        """Get the word as a string with all the possible hyphens inserted.

        :param word: unicode string of the word to hyphenate
        :param hyphen: unicode string used as hyphen character

        E.g. for the dutch word ``'lettergrepen'``, this method returns the
        unicode string ``'let-ter-gre-pen'``. The hyphen string to use can be
        given as the second parameter, that defaults to ``'-'``.

        """
        letters = list(word)
        for position in reversed(self.positions(word)):
            if position.data:
                # get the nonstandard hyphenation data
                change, index, cut = position.data
                index += position
                if word.isupper():
                    change = change.upper()
                letters[index:index + cut] = change.replace('=', hyphen)
            else:
                letters.insert(position, hyphen)

        return ''.join(letters)

    __call__ = iterate
