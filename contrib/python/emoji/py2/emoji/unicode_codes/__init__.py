# -*- coding: utf-8 -*-

from emoji.unicode_codes.data_dict import *


__all__ = [
    'EMOJI_UNICODE', 'UNICODE_EMOJI',
    'EMOJI_UNICODE_ENGLISH', 'UNICODE_EMOJI_ENGLISH',
    'EMOJI_ALIAS_UNICODE_ENGLISH', 'UNICODE_EMOJI_ALIAS_ENGLISH',
    'EMOJI_UNICODE_SPANISH', 'UNICODE_EMOJI_SPANISH',
    'EMOJI_UNICODE_PORTUGUESE', 'UNICODE_EMOJI_PORTUGUESE',
    'EMOJI_UNICODE_ITALIAN', 'UNICODE_EMOJI_ITALIAN',
    'EMOJI_UNICODE_FRENCH', 'UNICODE_EMOJI_FRENCH',
    'EMOJI_UNICODE_GERMAN', 'UNICODE_EMOJI_GERMAN',
    'EMOJI_DATA', 'STATUS'
]


def get_emoji_unicode_dict(lang):
    """ Get the EMOJI_UNICODE_{language} dict containing all fully-qualified and component emoji"""
    return {data[lang]: emj for emj, data in EMOJI_DATA.items() if lang in data and data['status'] <= STATUS['fully_qualified']}


def get_unicode_emoji_dict(lang):
    """ Get the UNICODE_EMOJI_{language} dict containing all emoji that have a name in {lang}"""
    return {emj: data[lang] for emj, data in EMOJI_DATA.items() if lang in data}


EMOJI_UNICODE_ENGLISH = get_emoji_unicode_dict('en')
UNICODE_EMOJI_ENGLISH = get_unicode_emoji_dict('en')

EMOJI_ALIAS_UNICODE_ENGLISH = dict(EMOJI_UNICODE_ENGLISH.items())
for emj, data in EMOJI_DATA.items():
    if 'alias' in data and data['status'] <= STATUS['fully_qualified']:
        for alias in data['alias']:
            EMOJI_ALIAS_UNICODE_ENGLISH[alias] = emj
UNICODE_EMOJI_ALIAS_ENGLISH = {v: k for k, v in EMOJI_ALIAS_UNICODE_ENGLISH.items()}

EMOJI_UNICODE_GERMAN = get_emoji_unicode_dict('de')
UNICODE_EMOJI_GERMAN = get_unicode_emoji_dict('de')

EMOJI_UNICODE_SPANISH = get_emoji_unicode_dict('es')
UNICODE_EMOJI_SPANISH = get_unicode_emoji_dict('es')

EMOJI_UNICODE_FRENCH = get_emoji_unicode_dict('fr')
UNICODE_EMOJI_FRENCH = get_unicode_emoji_dict('fr')

EMOJI_UNICODE_ITALIAN = get_emoji_unicode_dict('it')
UNICODE_EMOJI_ITALIAN = get_unicode_emoji_dict('it')

EMOJI_UNICODE_PORTUGUESE = get_emoji_unicode_dict('pt')
UNICODE_EMOJI_PORTUGUESE = get_unicode_emoji_dict('pt')

EMOJI_UNICODE = {
    'en': EMOJI_UNICODE_ENGLISH,
    'es': EMOJI_UNICODE_SPANISH,
    'pt': EMOJI_UNICODE_PORTUGUESE,
    'it': EMOJI_UNICODE_ITALIAN,
    'fr': EMOJI_UNICODE_FRENCH,
    'de': EMOJI_UNICODE_GERMAN,
}

UNICODE_EMOJI = {
    'en': UNICODE_EMOJI_ENGLISH,
    'es': UNICODE_EMOJI_SPANISH,
    'pt': UNICODE_EMOJI_PORTUGUESE,
    'it': UNICODE_EMOJI_ITALIAN,
    'fr': UNICODE_EMOJI_FRENCH,
    'de': UNICODE_EMOJI_GERMAN,
}
