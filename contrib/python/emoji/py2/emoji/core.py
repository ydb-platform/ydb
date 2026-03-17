# -*- coding: UTF-8 -*-


"""
emoji.core
~~~~~~~~~~

Core components for emoji.

"""

import re
import sys
import warnings

from emoji import unicode_codes

__all__ = [
    'emojize', 'demojize', 'get_emoji_regexp',
    'emoji_lis', 'distinct_emoji_lis', 'emoji_count',
    'replace_emoji', 'is_emoji', 'version',
    'emoji_list', 'distinct_emoji_list',
]

PY2 = sys.version_info[0] == 2

_EMOJI_REGEXP = None
_SEARCH_TREE = None
_DEFAULT_DELIMITER = ':'


class _DeprecatedParameter:
    pass


def _deprecation(message, stacklevel=3):
    message = (message + "\n") if message else ""
    warnings.warn("%sTo hide this warning, pin/downgrade the package to 'emoji~=1.6.3'" % (message, ), DeprecationWarning, stacklevel=stacklevel)


def _deprecation_removed(name, message=""):
    _deprecation("'emoji.%s' is deprecated and will be removed in version 2.0.0. %s" % (name, message), 4)


def emojize(
        string,
        use_aliases=_DeprecatedParameter,
        delimiters=(_DEFAULT_DELIMITER, _DEFAULT_DELIMITER),
        variant=None,
        language='en',
        version=None,
        handle_version=None
):
    """Replace emoji names in a string with unicode codes.
        >>> import emoji
        >>> print(emoji.emojize("Python is fun :thumbsup:", language='alias'))
        Python is fun 👍
        >>> print(emoji.emojize("Python is fun :thumbs_up:"))
        Python is fun 👍
        >>> print(emoji.emojize("Python is fun __thumbs_up__", delimiters = ("__", "__")))
        Python is fun 👍
        >>> print(emoji.emojize("Python is fun :red_heart:",variant="text_type"))
        Python is fun ❤
        >>> print(emoji.emojize("Python is fun :red_heart:",variant="emoji_type"))
        Python is fun ❤️ #red heart, not black heart

    :param string: String contains emoji names.
    :param use_aliases: (optional) Deprecated. Use language='alias' instead
    :param delimiters: (optional) Use delimiters other than _DEFAULT_DELIMITER
    :param variant: (optional) Choose variation selector between "base"(None), VS-15 ("text_type") and VS-16 ("emoji_type")
    :param language: Choose language of emoji name: language code 'es', 'de', etc. or 'alias'
        to use English aliases
    :param version: (optional) Max version. If set to an Emoji Version,
        all emoji above this version will be ignored.
    :param handle_version: (optional) Replace the emoji above ``version``
        instead of ignoring it. handle_version can be either a string or a
        callable; If it is a callable, it's passed the unicode emoji and the
        data dict from emoji.EMOJI_DATA and must return a replacement string
        to be used::

            handle_version(u'\\U0001F6EB', {
                'en' : ':airplane_departure:',
                'status' : fully_qualified,
                'E' : 1,
                'alias' : [u':flight_departure:'],
                'de': u':abflug:',
                'es': u':avión_despegando:',
                ...
            })

    :raises ValueError: if ``variant`` is neither None, 'text_type' or 'emoji_type'

    """

    if use_aliases is _DeprecatedParameter:
        use_aliases = False
    else:
        _deprecation("The parameter 'use_aliases' in emoji.emojize() is deprecated and will be removed in version 2.0.0. Use language='alias' instead.")

    if use_aliases or language == 'alias':
        if language not in ('en', 'alias'):
            warnings.warn("use_aliases=True is only supported for language='en'. "
                          "It is recommended to use emojize(string, language='alias') instead", stacklevel=2)
        use_aliases = True
        language = 'en'

    EMOJI_UNICODE = unicode_codes.EMOJI_ALIAS_UNICODE_ENGLISH if use_aliases else unicode_codes.EMOJI_UNICODE[language]
    pattern = re.compile(u'(%s[\\w\\-&.’”“()!#*+?–,/]+%s)' % delimiters, flags=re.UNICODE)

    def replace(match):
        mg = match.group(1)[len(delimiters[0]):-len(delimiters[1])]
        emj = EMOJI_UNICODE.get(_DEFAULT_DELIMITER + mg + _DEFAULT_DELIMITER)
        if emj is None:
            return match.group(1)

        if version is not None:
            if emj in unicode_codes.EMOJI_DATA and unicode_codes.EMOJI_DATA[emj]['E'] > version:
                if callable(handle_version):
                    return handle_version(emj, unicode_codes.EMOJI_DATA[emj])
                elif handle_version is not None:
                    return str(handle_version)
                else:
                    return ''

        if variant is None or 'variant' not in unicode_codes.EMOJI_DATA[emj]:
            return emj

        if emj[-1] == u'\uFE0E' or emj[-1] == u'\uFE0F':
            # Remove an existing variant
            emj = emj[0:-1]
        if variant == "text_type":
            return emj + u'\uFE0E'
        elif variant == "emoji_type":
            return emj + u'\uFE0F'
        else:
            raise ValueError("Parameter 'variant' must be either None, 'text_type' or 'emoji_type'")

    return pattern.sub(replace, string)


def demojize(
        string,
        use_aliases=_DeprecatedParameter,
        delimiters=(_DEFAULT_DELIMITER, _DEFAULT_DELIMITER),
        language='en',
        version=None,
        handle_version=None
):
    """Replace unicode emoji in a string with emoji shortcodes. Useful for storage.
        >>> import emoji
        >>> print(emoji.emojize("Python is fun :thumbs_up:"))
        Python is fun 👍
        >>> print(emoji.demojize(u"Python is fun 👍"))
        Python is fun :thumbs_up:
        >>> print(emoji.demojize(u"Unicode is tricky 😯", delimiters=("__", "__")))
        Unicode is tricky __hushed_face__

    :param string: String contains unicode characters. MUST BE UNICODE.
    :param use_aliases: (optional) Deprecated. Use language='alias' instead
    :param delimiters: (optional) User delimiters other than ``_DEFAULT_DELIMITER``
    :param language: Choose language of emoji name: language code 'es', 'de', etc. or 'alias'
        to use English aliases
    :param version: (optional) Max version. If set to an Emoji Version,
        all emoji above this version will be removed.
    :param handle_version: (optional) Replace the emoji above ``version``
        instead of removing it. handle_version can be either a string or a
        callable ``handle_version(emj: str, data: dict) -> str``; If it is
        a callable, it's passed the unicode emoji and the data dict from
        emoji.EMOJI_DATA and must return a replacement string  to be used.
        The passed data is in the form of::

            handle_version(u'\\U0001F6EB', {
                'en' : ':airplane_departure:',
                'status' : fully_qualified,
                'E' : 1,
                'alias' : [u':flight_departure:'],
                'de': u':abflug:',
                'es': u':avión_despegando:',
                ...
            })

    """

    if use_aliases is _DeprecatedParameter:
        use_aliases = False
    else:
        _deprecation("The parameter 'use_aliases' in emoji.demojize() is deprecated and will be removed in version 2.0.0. Use language='alias' instead.")

    if language == 'alias':
        language = 'en'
        use_aliases = True
    elif use_aliases and language != 'en':
        warnings.warn("use_aliases=True is only supported for language='en'. It is recommended to use demojize(string, language='alias') instead", stacklevel=2)
        language = 'en'

    tree = _get_search_tree()
    result = []
    i = 0
    length = len(string)
    while i < length:
        consumed = False
        char = string[i]
        if char in tree:
            j = i + 1
            sub_tree = tree[char]
            while j < length and string[j] in sub_tree:
                sub_tree = sub_tree[string[j]]
                j += 1
            if 'data' in sub_tree:
                emj_data = sub_tree['data']
                code_points = string[i:j]
                replace_str = None
                if version is not None and emj_data['E'] > version:
                    if callable(handle_version):
                        emj_data = emj_data.copy()
                        emj_data['match_start'] = i
                        emj_data['match_end'] = j
                        replace_str = handle_version(code_points, emj_data)
                    elif handle_version is not None:
                        replace_str = str(handle_version)
                    else:
                        replace_str = None
                elif language in emj_data:
                    if use_aliases and 'alias' in emj_data:
                        replace_str = delimiters[0] + emj_data['alias'][0][1:-1] + delimiters[1]
                    else:
                        replace_str = delimiters[0] + emj_data[language][1:-1] + delimiters[1]
                else:
                    # The emoji exists, but it is not translated, so we keep the emoji
                    replace_str = code_points

                i = j - 1
                consumed = True
                if replace_str:
                    result.append(replace_str)

        if not consumed and char != u'\ufe0e' and char != u'\ufe0f':
            result.append(char)
        i += 1

    return "".join(result)


def replace_emoji(string, replace='', language=_DeprecatedParameter, version=-1):
    """Replace unicode emoji in a customizable string.

    :param string: String contains unicode characters. MUST BE UNICODE.
    :param replace: (optional) replace can be either a string or a callable;
        If it is a callable, it's passed the unicode emoji and the data dict from
        emoji.EMOJI_DATA and must return a replacement string to be used.
        replace(str, dict) -> str
    :param version: (optional) Max version. If set to an Emoji Version,
        only emoji above this version will be replaced.
    :param language: (optional) Deprecated and has no effect
    """

    if language is not _DeprecatedParameter:
        _deprecation("The parameter 'language' in emoji.replace_emoji() is deprecated and will be removed in version 2.0.0.")

    if version > -1:
        def f(emj, emj_data):
            if emj_data['E'] <= version:
                return emj  # Do not replace emj
            if callable(replace):
                return replace(emj, emj_data)
            return str(replace)

        return demojize(string, language='en', version=-1, handle_version=f)
    else:
        return demojize(string, language='en', version=-1, handle_version=replace)


def get_emoji_regexp(language=None):
    """Returns compiled regular expression that matches all emojis defined in
    ``emoji.EMOJI_DATA``. The regular expression is only compiled once.

    :param language: (optional) Parameter is no longer used
    """

    _deprecation_removed("get_emoji_regexp()", "If you want to remove emoji from a string, consider the method emoji.replace_emoji(str, replace='').")

    global _EMOJI_REGEXP
    # Build emoji regexp once
    if _EMOJI_REGEXP is None:
        # Sort emojis by length to make sure multi-character emojis are
        # matched first
        emojis = sorted(unicode_codes.EMOJI_DATA, key=len, reverse=True)
        pattern = u'(' + u'|'.join(re.escape(u) for u in emojis) + u')'
        _EMOJI_REGEXP = re.compile(pattern)
    return _EMOJI_REGEXP


def emoji_lis(string, language=_DeprecatedParameter):
    """Returns the location and emoji in list of dict format.
        >>> emoji.emoji_lis("Hi, I am fine. 😁")
        [{'location': 15, 'emoji': '😁'}]

    :param language: (optional) Deprecated and has no effect
    """

    _deprecation_removed("emoji_lis()", "Use method emoji.emoji_list(str) instead.")

    _entities = []

    def f(emj, emj_data):
        _entities.append({
            'location': emj_data['match_start'],
            'emoji': emj,
        })

    demojize(string, language='en',
             version=-1, handle_version=f)
    return _entities


def emoji_list(string):
    """Returns the location and emoji in list of dict format.
        >>> emoji.emoji_list("Hi, I am fine. 😁")
        [{'match_start': 15, 'match_end': 16, 'emoji': '😁'}]y

    """

    _entities = []

    def f(emj, emj_data):
        _entities.append({
            'match_start': emj_data['match_start'],
            'match_end': emj_data['match_end'],
            'emoji': emj,
        })

    demojize(string, language='en',
             version=-1, handle_version=f)
    return _entities


def distinct_emoji_lis(string, language=_DeprecatedParameter):
    """Returns distinct list of emojis from the string.

    :param language: (optional) Deprecated and has no effect
    """

    _deprecation_removed("distinct_emoji_lis()", "Use method emoji.distinct_emoji_list(str) instead.")

    distinct_list = list(
        {e['emoji'] for e in emoji_lis(string)}
    )
    return distinct_list


def distinct_emoji_list(string):
    """Returns distinct list of emojis from the string.
    """

    distinct_list = list(
        {e['emoji'] for e in emoji_list(string)}
    )
    return distinct_list


def emoji_count(string, unique=False):
    """Returns the count of emojis in a string.

    :param unique: (optional) True if count only unique emojis
    """
    if unique:
        return len(distinct_emoji_lis(string))
    return len(emoji_lis(string))


def is_emoji(string):
    """Returns True if the string is an emoji"""
    return string in unicode_codes.EMOJI_DATA


def version(string):
    """Returns the Emoji Version of the emoji.
      See http://www.unicode.org/reports/tr51/#Versioning for more information.
        >>> emoji.version("😁")
        >>> 0.6
        >>> emoji.version(":butterfly:")
        >>> 3

    :param string: An emoji or a text containig an emoji
    :raises ValueError: if ``string`` does not contain an emoji
    """

    # Try dictionary lookup
    if string in unicode_codes.EMOJI_DATA:
        return unicode_codes.EMOJI_DATA[string]['E']

    if string in unicode_codes.EMOJI_UNICODE['en']:
        emj_code = unicode_codes.EMOJI_UNICODE['en'][string]
        if emj_code in unicode_codes.EMOJI_DATA:
            return unicode_codes.EMOJI_DATA[emj_code]['E']

    # Try to find first emoji in string
    version = []

    def f(e, emoji_data):
        version.append(emoji_data['E'])
        return ''
    replace_emoji(string, replace=f, version=-1)
    if version:
        return version[0]
    emojize(string, language='alias', version=-1, handle_version=f)
    if version:
        return version[0]
    for lang_code in unicode_codes.EMOJI_UNICODE:
        emojize(string, language=lang_code, version=-1, handle_version=f)
        if version:
            return version[0]

    raise ValueError("No emoji found in string")


def _get_search_tree():
    """
    Generate a search tree for demojize()
    Example of a search tree::

        EMOJI_DATA =
        {'a': {'en': ':Apple:'},
        'b': {'en': ':Bus:'},
        'ba': {'en': ':Bat:'},
        'band': {'en': ':Beatles:'},
        'bandit': {'en': ':Outlaw:'},
        'bank': {'en': ':BankOfEngland:'},
        'bb': {'en': ':BB-gun:'},
        'c': {'en': ':Car:'}}

        _SEARCH_TREE =
        {'a': {'data': {'en': ':Apple:'}},
        'b': {'a': {'data': {'en': ':Bat:'},
                    'n': {'d': {'data': {'en': ':Beatles:'},
                                'i': {'t': {'data': {'en': ':Outlaw:'}}}},
                        'k': {'data': {'en': ':BankOfEngland:'}}}},
            'b': {'data': {'en': ':BB-gun:'}},
            'data': {'en': ':Bus:'}},
        'c': {'data': {'en': ':Car:'}}}

                   _SEARCH_TREE
                 /     |        ⧵
               /       |          ⧵
            a          b             c
            |        / |  ⧵          |
            |       /  |    ⧵        |
        :Apple:   ba  :Bus:  bb     :Car:
                 /  ⧵         |
                /    ⧵        |
              :Bat:    ban     :BB-gun:
                     /     ⧵
                    /       ⧵
                 band       bank
                /   ⧵         |
               /     ⧵        |
            bandi :Beatles:  :BankOfEngland:
               |
            bandit
               |
           :Outlaw:


    """
    global _SEARCH_TREE
    if _SEARCH_TREE is None:
        _SEARCH_TREE = {}
        for emj in unicode_codes.EMOJI_DATA:
            sub_tree = _SEARCH_TREE
            lastidx = len(emj) - 1
            for i, char in enumerate(emj):
                if char not in sub_tree:
                    sub_tree[char] = {}
                sub_tree = sub_tree[char]
                if i == lastidx:
                    sub_tree['data'] = unicode_codes.EMOJI_DATA[emj]
    return _SEARCH_TREE
