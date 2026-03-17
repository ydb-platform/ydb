from collections import Counter
import logging
import re
import unicodedata

from .base import registry
from .conf import get_setting
from .discover import autodiscover
from .exceptions import (
    LanguageCodeError,
    LanguageDetectionError,
    LanguagePackNotFound,
)

LOGGER = logging.getLogger(__name__)

__title__ = 'transliterate.utils'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = (
    'detect_language',
    'get_available_language_codes',
    'get_available_language_packs',
    'get_translit_function',
    'slugify',
    'suggest',
    'translit',
)


def _(val):
    """Fake translation wrapper."""
    return val


def ensure_autodiscover():
    """Ensure autodiscover."""
    # Running autodiscover if registry is empty
    if not registry.registry:
        autodiscover()


def get_translit_function(language_code):
    """Return translit function for the language given.

    :param str language_code:
    :return callable:
    """
    ensure_autodiscover()

    cls = registry.get(language_code)
    if cls is None:
        raise LanguagePackNotFound(
            _("Language pack for code %s is not found." % language_code)
        )

    language_pack = cls()
    return language_pack.translit


def translit(value, language_code=None, reversed=False, strict=False):
    """Transliterate the text for the language given.

    Language code is optional in case of reversed translations (from some
    script to latin).

    :param str value:
    :param str language_code:
    :param bool reversed: If set to True, reversed translation is made.
    :param bool strict: If given, all that are not found in the
        transliteration pack, are simply stripped out.
    :return str:
    """
    ensure_autodiscover()

    if language_code is None and reversed is False:
        raise LanguageCodeError(
            _("``language_code`` is optional with ``reversed`` set to True "
              "only.")
        )

    if language_code is None:
        language_code = detect_language(value, fail_silently=False)

    cls = registry.get(language_code)

    if cls is None:
        raise LanguagePackNotFound(
            _("Language pack for code %s is not found." % language_code)
        )

    language_pack = cls()
    return language_pack.translit(value, reversed=reversed, strict=strict)


def suggest(value, language_code=None, reversed=False, limit=None):
    """Suggest possible variants.

    :param str value:
    :param str language_code:
    :param bool reversed: If set to True, reversed translation is made.
    :param int limit: Limit number of suggested variants.
    :return list:
    """
    ensure_autodiscover()

    if language_code is None and reversed is False:
        raise LanguageCodeError(
            _("``language_code`` is optional with ``reversed`` set to True "
              "only.")
        )

    cls = registry.get(language_code)

    if cls is None:
        raise LanguagePackNotFound(
            _("Language pack for code %s is not found." % language_code)
        )

    language_pack = cls()

    return language_pack.suggest(value, reversed=reversed, limit=limit)


def get_available_language_codes():
    """Get list of language codes for registered language packs.

    :return list:
    """
    ensure_autodiscover()

    return [key for (key, val) in registry.registry.items()]


def get_available_language_packs():
    """Get list of registered language packs.

    :return list:
    """
    ensure_autodiscover()

    return [val for (key, val) in registry.registry.items()]


def get_language_pack(language_code):
    """Get registered language pack by language code given.

    :param str language_code:
    :return transliterate.base.TranslitLanguagePack: Returns None on failure.
    """
    ensure_autodiscover()
    return registry.registry.get(language_code, None)


# Strips numbers from unicode string.
def strip_numbers(text):
    """Strip numbers from text."""
    return ''.join(filter(lambda u: not u.isdigit(), text))


def extract_most_common_words(text, num_words=None):
    """Extract most common words.

    :param unicode text:
    :param int num_words:
    :return list:
    """
    if num_words is None:
        num_words = get_setting('LANGUAGE_DETECTION_MAX_NUM_KEYWORDS')

    text = strip_numbers(text)
    counter = Counter()
    for word in text.split(' '):
        if len(word) > 1:
            counter[word] += 1
    return counter.most_common(num_words)


def detect_language(text, num_words=None, fail_silently=True,
                    heavy_check=False):
    """Detect the language from the value given.

    Detect the language from the value given based on ranges defined in active
    language packs.

    :param unicode value: Input string.
    :param int num_words: Number of words to base decision on.
    :param bool fail_silently:
    :param bool heavy_check: If given, heavy checks would be applied when
        simple checks don't give any results. Heavy checks are language
        specific and do not apply to a common logic. Heavy language detection
        is defined in the ``detect`` method of each language pack.
    :return str: Language code.
    """
    ensure_autodiscover()

    if num_words is None:
        num_words = get_setting('LANGUAGE_DETECTION_MAX_NUM_KEYWORDS')

    most_common_words = extract_most_common_words(text, num_words=num_words)

    counter = Counter()

    available_language_packs = get_available_language_packs()

    for word, occurrences in most_common_words:
        for letter in word:
            for language_pack in available_language_packs:
                if language_pack.detectable and language_pack.contains(letter):
                    counter[language_pack.language_code] += 1
                    continue
    try:
        return counter.most_common(1)[0][0]
    except Exception as err:
        if get_setting('DEBUG'):
            LOGGER.debug(str(err))

    if not fail_silently:
        raise LanguageDetectionError(
            _("""Can't detect language for the text "%s" given.""") % text
        )


def slugify(text, language_code=None):
    """Slugify the given text.

    If no ``language_code`` is given, auto-detect the language code from
    text given.

    :param str text:
    :param str language_code:
    :return str:
    """
    if not language_code:
        language_code = detect_language(text)
    if language_code:
        transliterated_text = translit(text, language_code, reversed=True)
        slug = unicodedata.normalize('NFKD', transliterated_text) \
                          .encode('ascii', 'ignore') \
                          .decode('ascii')
        slug = re.sub(r'[^\w\s-]', '', slug).strip().lower()
        return re.sub(r'[-\s]+', '-', slug)
