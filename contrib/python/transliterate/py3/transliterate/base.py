# -*- coding: utf-8 -*-

import re
import unicodedata

import six

from .exceptions import (
    ImproperlyConfigured,
    InvalidRegistryItemType
)

__title__ = 'transliterate.base'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = (
    'registry',
    'TranslitLanguagePack',
)


class TranslitLanguagePack(object):
    """Base language pack.

    The attributes below shall be defined in every language pack.

    ``language_code``: Language code (obligatory). Example value: 'hy', 'ru'.
    ``language_name``: Language name (obligatory). Example value: 'Armenian',
        'Russian'.
    ``character_ranges``: Character ranges that are specific to the language.
        When making a pack, check `this
        <http://en.wikipedia.org/wiki/List_of_Unicode_characters>`_ page for
        the ranges.
    ``mapping``: Mapping  (obligatory). A tuple, consisting of two strings
        (source and target). Example value: (u'abc', u'աբց').
    ``reversed_specific_mapping``: Specific mapping (one direction only) used
        when transliterating from target script to source script (reversed
        transliteration).
    ՝՝pre_processor_mapping՝՝: Pre processor mapping (optional). A dictionary
        mapping for letters that can't be represented by a single latin letter.
    ՝՝reversed_specific_pre_processor_mapping՝՝: Pre processor mapping (
        optional). A dictionary mapping for letters that can't be represented
        by a single latin letter (reversed transliteration).

    :example:
>>>    class ArmenianLanguagePack(TranslitLanguagePack):
>>>    language_code = "hy"
>>>    language_name = "Armenian"
>>>    character_ranges = ((0x0530, 0x058F), (0xFB10, 0xFB1F))
>>>    mapping = (
>>>        u"abgdezilxkhmjnpsvtrcq&ofABGDEZILXKHMJNPSVTRCQOF", # Source script
>>>        u"աբգդեզիլխկհմյնպսվտրցքևօֆԱԲԳԴԵԶԻԼԽԿՀՄՅՆՊՍՎՏՐՑՔՕՖ", # Target script
>>>    )
>>>    reversed_specific_mapping = (
>>>        u"ռՌ",
>>>        u"rR"
>>>    )
>>>    pre_processor_mapping = {
>>>        # lowercase
>>>        u"e'": u"է",
>>>        u"y": u"ը",
>>>        u"th": u"թ",
>>>        u"jh": u"ժ",
>>>        u"ts": u"ծ",
>>>        u"dz": u"ձ",
>>>        u"gh": u"ղ",
>>>        u"tch": u"ճ",
>>>        u"sh": u"շ",
>>>        u"vo": u"ո",
>>>        u"ch": u"չ",
>>>        u"dj": u"ջ",
>>>        u"ph": u"փ",
>>>        u"u": u"ու",
>>>
>>>        # uppercase
>>>        u"E'": u"Է",
>>>        u"Y": u"Ը",
>>>        u"Th": u"Թ",
>>>        u"Jh": u"Ժ",
>>>        u"Ts": u"Ծ",
>>>        u"Dz": u"Ձ",
>>>        u"Gh": u"Ղ",
>>>        u"Tch": u"Ճ",
>>>        u"Sh": u"Շ",
>>>        u"Vo": u"Ո",
>>>        u"Ch": u"Չ",
>>>        u"Dj": u"Ջ",
>>>        u"Ph": u"Փ",
>>>        u"U": u"Ու"
>>>    }
>>>    reversed_specific_pre_processor_mapping = {
>>>        u"ու": u"u",
>>>        u"Ու": u"U"
>>>    }
    Note, that in Python 3 you won't be using u prefix before the strings.
    """

    language_code = None
    language_name = None
    character_ranges = None
    mapping = None
    reversed_specific_mapping = None

    reversed_pre_processor_mapping_keys = []

    reversed_specific_pre_processor_mapping = None
    reversed_specific_pre_processor_mapping_keys = []

    pre_processor_mapping = None
    pre_processor_mapping_keys = []

    detectable = False
    characters = None
    reversed_characters = None

    def __init__(self):
        try:
            assert self.language_code is not None
            assert self.language_name is not None
            assert self.mapping
        except AssertionError:
            raise ImproperlyConfigured(
                "You should define ``language_code``, ``language_name`` and "
                "``mapping`` properties in your subclassed "
                "``TranslitLanguagePack`` class."
            )

        super(TranslitLanguagePack, self).__init__()

        # Creating a translation table from the mapping set.
        self.translation_table = {}

        for key, val in zip(*self.mapping):
            self.translation_table.update({ord(key): ord(val)})

        # Creating a reversed translation table.
        self.reversed_translation_table = dict(
            zip(self.translation_table.values(), self.translation_table.keys())
        )

        # If any pre-processor rules defined, reversing them for later use.
        if self.pre_processor_mapping:
            self.reversed_pre_processor_mapping = dict(
                zip(
                    self.pre_processor_mapping.values(),
                    self.pre_processor_mapping.keys())
                )
            self.pre_processor_mapping_keys = self.pre_processor_mapping.keys()
            self.reversed_pre_processor_mapping_keys = \
                self.pre_processor_mapping.values()

        else:
            self.reversed_pre_processor_mapping = None

        if self.reversed_specific_mapping:
            self.reversed_specific_translation_table = {}
            for key, val in zip(*self.reversed_specific_mapping):
                self.reversed_specific_translation_table.update(
                    {ord(key): ord(val)}
                )

        if self.reversed_specific_pre_processor_mapping:
            self.reversed_specific_pre_processor_mapping_keys = \
                self.reversed_specific_pre_processor_mapping.keys()

        self._characters = '[^]'

        if self.characters is not None:
            self._characters = '[^{0}]'.format(
                '\\'.join(list(self.characters))
            )

        self._reversed_characters = '[^]'
        if self.reversed_characters is not None:
            self._reversed_characters = \
                '[^{0}]'.format('\\'.join(list(self.characters)))

    def translit(self, value, reversed=False, strict=False,
                 fail_silently=True):
        """Transliterate the given value according to the rules.

        Rules are set in the transliteration pack.

        :param str value:
        :param bool reversed:
        :param bool strict:
        :param bool fail_silently:
        :return str:
        """
        if not six.PY3:
            value = unicode(value)

        if reversed:
            # Handling reversed specific translations (one side only).
            if self.reversed_specific_mapping:
                value = value.translate(
                    self.reversed_specific_translation_table
                )

            if self.reversed_specific_pre_processor_mapping:
                for rule in self.reversed_specific_pre_processor_mapping_keys:
                    value = value.replace(
                        rule,
                        self.reversed_specific_pre_processor_mapping[rule]
                    )

            # Handling pre-processor mappings.
            if self.reversed_pre_processor_mapping:
                for rule in self.reversed_pre_processor_mapping_keys:
                    value = value.replace(
                        rule,
                        self.reversed_pre_processor_mapping[rule]
                    )

            return value.translate(self.reversed_translation_table)

        if self.pre_processor_mapping:
            for rule in self.pre_processor_mapping_keys:
                value = value.replace(rule, self.pre_processor_mapping[rule])
        res = value.translate(self.translation_table)

        if strict:
            res = self._make_strict(value=res,
                                    reversed=reversed,
                                    fail_silently=fail_silently)

        return res

    def _make_strict(self, value, reversed=False, fail_silently=True):
        """Strip out unnecessary characters from the string.

        :param string value:
        :param bool reversed:
        :param bool fail_silently:
        :return string:
        """
        try:
            return self.make_strict(value, reversed)
        except Exception as err:
            if fail_silently:
                return value
            else:
                raise err

    def make_strict(self, value, reversed=False):
        """Strip out unnecessary characters from the string.

        :param string value:
        :param bool reversed:
        :return string:
        """
        if reversed:
            if self.reversed_characters:
                # Make strict based on the ``reversed_characters``.
                value = re.sub(self._reversed_characters, '', value)
            else:
                # Make strict based on the ``character_ranges`` specified.
                pass
        else:
            if self.characters:
                # Make strict based on the ``characters``.
                value = re.sub(self._characters, '', value)
            else:
                # Make strict based on the ``character_ranges`` specified.
                pass

        return value

    @classmethod
    def contains(cls, character):
        """Check if given character belongs to the language pack.

        :return bool:
        """
        if cls.character_ranges:
            char_num = unicodedata.normalize('NFC', character)
            char_num = ord(char_num)
            for character_range in cls.character_ranges:
                range_lower = character_range[0]
                range_upper = character_range[1]
                if char_num >= range_lower and char_num <= range_upper:
                    return True
        return False

    @classmethod
    def suggest(value, reversed=False, limit=None):
        """Suggest possible variants (some sort of auto-complete).

        :param str value:
        :param int limit: Limit number of suggested variants.
        :return list:
        """
        # TODO

    @classmethod
    def detect(text, num_words=None):
        """Detect the language.

        Heavy language detection, which is activated for languages that are
        harder detect (like Russian Cyrillic and Ukrainian Cyrillic).

        :param unicode value: Input string.
        :param int num_words: Number of words to base decision on.
        :return bool: True if detected and False otherwise.
        """
        # TODO


class TranslitRegistry(object):
    """Language pack registry."""

    def __init__(self):
        self._registry = {}
        self._forced = []

    @property
    def registry(self):
        """Registry."""
        return self._registry

    def register(self, cls, force=False):
        """Register the language pack in the registry.

        :param transliterate.base.LanguagePack cls: Subclass of
            ``transliterate.base.LanguagePack``.
        :param bool force: If set to True, item stays forced. It's not possible
            to un-register a forced item.
        :return bool: True if registered and False otherwise.
        """
        if not issubclass(cls, TranslitLanguagePack):
            raise InvalidRegistryItemType(
                "Invalid item type `%s` for registry `%s`",
                cls,
                self.__class__
            )

        # If item has not been forced yet, add/replace its' value in the
        # registry.
        if force:

            if cls.language_code not in self._forced:
                self._registry[cls.language_code] = cls
                self._forced.append(cls.language_code)
                return True
            else:
                return False

        else:

            if cls.language_code in self._registry:
                return False
            else:
                self._registry[cls.language_code] = cls
                return True

    def unregister(self, cls):
        """Un-registers an item from registry.

        :param transliterate.base.LanguagePack cls: Subclass of
            ``transliterate.base.LanguagePack``.
        :return bool: True if unregistered and False otherwise.
        """
        if not issubclass(cls, TranslitLanguagePack):
            raise InvalidRegistryItemType(
                "Invalid item type `%s` for registry `%s`",
                cls,
                self.__class__
            )

        # Only non-forced items are allowed to be unregistered.
        if cls.language_code in self._registry \
                and cls.language_code not in self._forced:

            self._registry.pop(cls.language_code)
            return True
        else:
            return False

    def get(self, language_code, default=None):
        """Get the given language pack from the registry.

        :param str language_code:
        :return transliterate.base.LanguagePack: Subclass of
            ``transliterate.base.LanguagePack``.
        """
        return self._registry.get(language_code, default)


# Register languages by calling registry.register()
registry = TranslitRegistry()
