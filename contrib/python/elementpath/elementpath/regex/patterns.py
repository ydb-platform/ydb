#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
"""
Parse and translate XML Schema regular expressions to Python regex syntax.
"""
import re
from sys import maxunicode

from .codepoints import RegexError
from .unicode_subsets import UnicodeSubset, unicode_subset
from .character_classes import CharacterClass, I_SHORTCUT_REPLACE, C_SHORTCUT_REPLACE

HYPHENS_PATTERN = re.compile(r'(?<!\\)--')
INVALID_HYPHEN_PATTERN = re.compile(r'[^\\]-[^\\]-[^\\]')
DIGITS_PATTERN = re.compile(r'\d+')
QUANTIFIER_PATTERN = re.compile(r'{\d+(,(\d+)?)?}')
FORBIDDEN_ESCAPES_NOREF_PATTERN = re.compile(
    r'(?<!\\)\\(U[\da-fA-F]{8}|u[\da-fA-F]{4}|x[\da-fA-F]{2}|o{\d+}|\d+|A|Z|z|B|b|o|0\d{2})'
)
FORBIDDEN_ESCAPES_REF_PATTERN = re.compile(
    r'(?<!\\)\\(U[\da-fA-F]{8}|u[\da-fA-F]{4}|x[\da-fA-F]{2}|o{\d+}|A|Z|z|B|b|o|0\d{2})'
)


def translate_pattern(pattern: str, flags: int = 0, xsd_version: str = '1.0',
                      back_references: bool = True, lazy_quantifiers: bool = True,
                      anchors: bool = True) -> str:
    """
    Translates a pattern regex expression to a Python regex pattern. With default
    options the translator processes XPath 2.0/XQuery 1.0 regex patterns. For XML
    Schema patterns set all boolean options to `False`.

    :param pattern: the source XML Schema regular expression.
    :param flags: regex flags as represented by Python's re module.
    :param xsd_version: apply regex rules of a specific XSD version, '1.0' for default.
    :param back_references: if `True` supports back-references and capturing groups.
    :param lazy_quantifiers: if `True` supports lazy quantifiers (\\*?, +?).
    :param anchors: if `True` supports ^ and $ anchors, otherwise the translated \
    pattern is anchored to its boundaries and anchors are treated as normal characters.
    """
    pos: int
    msg: str

    def parse_character_class() -> CharacterClass:
        nonlocal pos
        nonlocal msg

        pos += 1
        if pattern[pos] == '^':
            pos += 1
            negative = True
        else:
            negative = False

        char_class_pos = pos
        while True:
            if pattern[pos] == '[':
                msg = "invalid character '[' at position {}: {!r}"
                raise RegexError(msg.format(pos, pattern))
            elif pattern[pos] == '\\':
                if pattern[pos + 1].isdigit():
                    msg = "illegal back-reference in character class at position {}: {!r}"
                    raise RegexError(msg.format(pos, pattern))
                pos += 2
            elif pattern[pos] == ']' or pattern[pos:pos + 2] == '-[':
                if pos == char_class_pos:
                    msg = "empty character class at position {}: {!r}"
                    raise RegexError(msg.format(pos, pattern))

                char_class_pattern = pattern[char_class_pos:pos]
                if HYPHENS_PATTERN.search(char_class_pattern) and pos - char_class_pos > 2:
                    msg = "invalid character range '--' at position {}: {!r}"
                    raise RegexError(msg.format(pos, pattern))

                if xsd_version == '1.0':
                    hyphen_match = INVALID_HYPHEN_PATTERN.search(char_class_pattern)
                    if hyphen_match is not None:
                        hyphen_pos = char_class_pos + hyphen_match.span()[1] - 2
                        msg = "unescaped character '-' at position {}: {!r}"
                        raise RegexError(msg.format(hyphen_pos, pattern))

                char_class = CharacterClass(char_class_pattern, xsd_version)
                if negative:
                    char_class.complement()
                break  # pragma: no cover
            else:
                pos += 1

        if pattern[pos] != ']':
            # Parse a group subtraction
            pos += 1
            subtracted_class = parse_character_class()
            pos += 1
            char_class -= subtracted_class

        return char_class

    group_open_char = '(' if back_references else '(?:'
    regex = [] if anchors else ['^%s' % group_open_char]
    pos = 0
    pattern_len = len(pattern)
    total_groups = 0
    nested_groups = 0
    dot_all = flags & re.DOTALL

    if back_references:
        match = FORBIDDEN_ESCAPES_REF_PATTERN.search(pattern)
    else:
        match = FORBIDDEN_ESCAPES_NOREF_PATTERN.search(pattern)

    if match:
        msg = "not allowed escape sequence {!r} at position {}: {!r}"
        raise RegexError(msg.format(match.group(), match.span()[0], pattern))

    while pos < pattern_len:
        ch = pattern[pos]
        if ch == '.':
            regex.append(ch if dot_all else r'[^\r\n]')
        elif ch in ('^', '$'):
            if not anchors:
                regex.append(r'\%s' % ch)
            elif ch == '^':
                regex.append(r'(?<!\n\Z)^' if flags & re.MULTILINE else '^')
            else:
                regex.append('$' if flags & re.MULTILINE else r'$(?!\n\Z)')

        elif ch == '[':
            try:
                char_class_repr = str(parse_character_class())
            except IndexError:
                msg = "unterminated character class at position {}: {!r}"
                raise RegexError(msg.format(pos, pattern))
            else:
                if char_class_repr == '[]':
                    regex.append(r'[^\w\W]')
                else:
                    regex.append(char_class_repr)

        elif ch == '{':
            if pos == 0:
                msg = "unexpected quantifier {!r} at position {}: {!r}"
                raise RegexError(msg.format(ch, pos, pattern))

            match = QUANTIFIER_PATTERN.match(pattern[pos:])
            if match is None:
                msg = "invalid quantifier {!r} at position {}: {!r}"
                raise RegexError(msg.format(ch, pos, pattern))

            if regex and regex[-1] in ('^', r'(?<!\n\Z)^', '$', r'$(?!\n\Z)'):
                # ^{n} or ${n} allowed but useless. Invalid in Python re
                # so encapsulate '^'/'$' inside a non-capturing group.
                regex[-1] = f'(?:{regex[-1]})'

            regex.append(match.group())
            pos += len(match.group())
            if pos < pattern_len and pattern[pos] in '?+*':
                if not lazy_quantifiers or pattern[pos] != '?':
                    msg = "unexpected meta character {!r} at position {}: {!r}"
                    raise RegexError(msg.format(pattern[pos], pos, pattern))
            continue  # pragma: no cover

        elif ch == '(':
            if pattern[pos:pos + 2] == '(?' and pattern[pos:pos + 3] != '(?:':
                msg = "invalid '(?...)' extension notation at position {}: {!r}"
                raise RegexError(msg.format(pos, pattern))

            total_groups += 1
            nested_groups += 1
            regex.append(group_open_char)

        elif ch == ']':
            msg = "unexpected meta character {!r} at position {}: {!r}"
            raise RegexError(msg.format(ch, pos, pattern))

        elif ch == ')':
            if nested_groups == 0:
                msg = "unbalanced parenthesis ')' at position {}: {!r}"
                raise RegexError(msg.format(pos, pattern))

            nested_groups -= 1
            regex.append(ch)

        elif ch in ('?', '*', '+'):
            if pos == 0:
                msg = "unexpected quantifier {!r} at position {}: {!r}"
                raise RegexError(msg.format(ch, pos, pattern))
            elif pos < pattern_len - 1 and pattern[pos + 1] in '?+*{':
                if not lazy_quantifiers or pattern[pos + 1] != '?':
                    msg = "unexpected meta character {!r} at position {}: {!r}"
                    raise RegexError(msg.format(pattern[pos + 1], pos + 1, pattern))

            if regex and regex[-1] in ('^', r'(?<!\n\Z)^', '$', r'$(?!\n\Z)'):
                # ^*/^+/^? or $*/$+/$? allowed but useless. Invalid in Python re
                # so encapsulate '^'/'$' inside a non-capturing group.
                regex[-1] = f'(?:{regex[-1]})'

            regex.append(ch)

        elif ch == '\\':
            pos += 1
            if flags & re.VERBOSE:
                while pos < pattern_len and pattern[pos] == ' ':
                    pos += 1

            if pos >= pattern_len:
                regex.append('\\')
            elif pattern[pos].isdigit():
                regex.append('\\%s' % pattern[pos])
                reference = DIGITS_PATTERN.match(pattern[pos:]).group()  # type: ignore[union-attr]
                if len(reference) > 1:
                    k = 0
                    for k in range(1, len(reference)):
                        if total_groups < int(reference[:k + 1]):
                            regex.append('[%s]' % pattern[pos + k])
                            break
                        else:
                            regex.append(pattern[pos + k])
                    pos += k  # pragma: no cover
            elif pattern[pos] == 'i':
                regex.append('[%s]' % I_SHORTCUT_REPLACE)
            elif pattern[pos] == 'I':
                regex.append('[^%s]' % I_SHORTCUT_REPLACE)
            elif pattern[pos] == 'c':
                regex.append('[%s]' % C_SHORTCUT_REPLACE)
            elif pattern[pos] == 'C':
                regex.append('[^%s]' % C_SHORTCUT_REPLACE)
            elif pattern[pos] in 'pP':
                block_pos = pos - 1
                try:
                    if pattern[pos + 1] != '{':
                        raise RegexError("a '{' expected, found %r." % pattern[pos + 1])
                    while pattern[pos] != '}':
                        pos += 1
                except (IndexError, ValueError):
                    msg = "truncated unicode block escape at position {}: {!r}"
                    raise RegexError(msg.format(block_pos, pattern))

                block_name = pattern[block_pos + 3:pos]
                if flags & re.VERBOSE:
                    # spaces are completely collapsed in verbose regex patterns
                    block_name = block_name.replace(' ', '')

                try:
                    p_shortcut_set = unicode_subset(block_name)
                except RegexError:
                    # XSD 1.1 supports Is prefix to match Unicode blocks
                    if xsd_version == '1.0' or not block_name.startswith('Is'):
                        raise
                    p_shortcut_group = '[%s]' % UnicodeSubset([(0, maxunicode)])
                else:
                    if pattern[block_pos + 1] == 'p':
                        p_shortcut_group = '[%s]' % p_shortcut_set
                    else:
                        p_shortcut_group = '[^%s]' % p_shortcut_set

                if flags & re.IGNORECASE:
                    regex.append('(?-i:%s)' % p_shortcut_group)
                else:
                    regex.append(p_shortcut_group)

            else:
                regex.append('\\%s' % pattern[pos])
        else:
            regex.append(ch)
        pos += 1

    if nested_groups > 0:
        raise RegexError("unterminated subpattern in expression: %r" % pattern)

    if not anchors:
        regex.append(r')$(?!\n\Z)')
    return ''.join(regex)
