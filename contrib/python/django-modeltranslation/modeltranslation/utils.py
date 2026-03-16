# -*- coding: utf-8 -*-
from contextlib import contextmanager

import six
from django.utils.encoding import force_str
from django.utils.translation import get_language as _get_language
from django.utils.translation import get_language_info
from django.utils.functional import lazy

from modeltranslation import settings


def get_language():
    """
    Return an active language code that is guaranteed to be in
    settings.LANGUAGES (Django does not seem to guarantee this for us).
    """
    lang = _get_language()
    if lang is None:  # Django >= 1.8
        return settings.DEFAULT_LANGUAGE
    if lang not in settings.AVAILABLE_LANGUAGES and '-' in lang:
        lang = lang.split('-')[0]
    if lang in settings.AVAILABLE_LANGUAGES:
        return lang
    return settings.DEFAULT_LANGUAGE


def get_language_bidi(lang):
    """
    Check if a language is bi-directional.
    """
    lang_info = get_language_info(lang)
    return lang_info['bidi']


def get_translation_fields(field):
    """
    Returns a list of localized fieldnames for a given field.
    """
    return [build_localized_fieldname(field, lang) for lang in settings.AVAILABLE_LANGUAGES]


def build_localized_fieldname(field_name, lang):
    if lang == 'id':
        # The 2-letter Indonesian language code is problematic with the
        # current naming scheme as Django foreign keys also add "id" suffix.
        lang = 'ind'
    return str('%s_%s' % (field_name, lang.replace('-', '_')))


def _build_localized_verbose_name(verbose_name, lang):
    if lang == 'id':
        lang = 'ind'
    return force_str('%s [%s]') % (force_str(verbose_name), lang)


build_localized_verbose_name = lazy(_build_localized_verbose_name, six.text_type)


def _join_css_class(bits, offset):
    if '-'.join(bits[-offset:]) in settings.AVAILABLE_LANGUAGES + ['en-us']:
        return '%s-%s' % ('_'.join(bits[: len(bits) - offset]), '_'.join(bits[-offset:]))
    return ''


def build_css_class(localized_fieldname, prefix=''):
    """
    Returns a css class based on ``localized_fieldname`` which is easily
    splittable and capable of regionalized language codes.

    Takes an optional ``prefix`` which is prepended to the returned string.
    """
    bits = localized_fieldname.split('_')
    css_class = ''
    if len(bits) == 1:
        css_class = str(localized_fieldname)
    elif len(bits) == 2:
        # Fieldname without underscore and short language code
        # Examples:
        # 'foo_de' --> 'foo-de',
        # 'bar_en' --> 'bar-en'
        css_class = '-'.join(bits)
    elif len(bits) > 2:
        # Try regionalized language code
        # Examples:
        # 'foo_es_ar' --> 'foo-es_ar',
        # 'foo_bar_zh_tw' --> 'foo_bar-zh_tw'
        css_class = _join_css_class(bits, 2)
        if not css_class:
            # Try short language code
            # Examples:
            # 'foo_bar_de' --> 'foo_bar-de',
            # 'foo_bar_baz_de' --> 'foo_bar_baz-de'
            css_class = _join_css_class(bits, 1)
    return '%s-%s' % (prefix, css_class) if prefix else css_class


def unique(seq):
    """
    Returns a generator yielding unique sequence members in order

    A set by itself will return unique values without any regard for order.

    >>> list(unique([1, 2, 3, 2, 2, 4, 1]))
    [1, 2, 3, 4]
    """
    seen = set()
    return (x for x in seq if x not in seen and not seen.add(x))


def resolution_order(lang, override=None):
    """
    Return order of languages which should be checked for parameter language.
    First is always the parameter language, later are fallback languages.
    Override parameter has priority over FALLBACK_LANGUAGES.
    """
    if not settings.ENABLE_FALLBACKS:
        return (lang,)
    if override is None:
        override = {}
    fallback_for_lang = override.get(lang, settings.FALLBACK_LANGUAGES.get(lang, ()))
    fallback_def = override.get('default', settings.FALLBACK_LANGUAGES['default'])
    order = (lang,) + fallback_for_lang + fallback_def
    return tuple(unique(order))


@contextmanager
def auto_populate(mode='all'):
    """
    Overrides translation fields population mode (population mode decides which
    unprovided translations will be filled during model construction / loading).

    Example:

        with auto_populate('all'):
            s = Slugged.objects.create(title='foo')
        s.title_en == 'foo' // True
        s.title_de == 'foo' // True

    This method may be used to ensure consistency loading untranslated fixtures,
    with non-default language active:

        with auto_populate('required'):
            call_command('loaddata', 'fixture.json')
    """
    current_population_mode = settings.AUTO_POPULATE
    settings.AUTO_POPULATE = mode
    try:
        yield
    finally:
        settings.AUTO_POPULATE = current_population_mode


@contextmanager
def fallbacks(enable=True):
    """
    Temporarily switch all language fallbacks on or off.

    Example:

        with fallbacks(False):
            lang_has_slug = bool(self.slug)

    May be used to enable fallbacks just when they're needed saving on some
    processing or check if there is a value for the current language (not
    knowing the language)
    """
    current_enable_fallbacks = settings.ENABLE_FALLBACKS
    settings.ENABLE_FALLBACKS = enable
    try:
        yield
    finally:
        settings.ENABLE_FALLBACKS = current_enable_fallbacks


def parse_field(setting, field_name, default):
    """
    Extract result from single-value or dict-type setting like fallback_values.
    """
    if isinstance(setting, dict):
        return setting.get(field_name, default)
    else:
        return setting
