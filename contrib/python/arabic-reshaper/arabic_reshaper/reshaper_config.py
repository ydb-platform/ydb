# -*- coding: utf-8 -*-

# This work is licensed under the MIT License.
# To view a copy of this license, visit https://opensource.org/licenses/MIT

# Written by Abdullah Diab (mpcabd)
# Email: mpcabd@gmail.com
# Website: http://mpcabd.xyz

import os

from configparser import ConfigParser

from .letters import (UNSHAPED, ISOLATED, LETTERS_ARABIC)
from .ligatures import (SENTENCES_LIGATURES,
                        WORDS_LIGATURES,
                        LETTERS_LIGATURES)

try:
    from fontTools.ttLib import TTFont
    with_font_config = True
except ImportError:
    with_font_config = False

ENABLE_NO_LIGATURES = 0b000
ENABLE_SENTENCES_LIGATURES = 0b001
ENABLE_WORDS_LIGATURES = 0b010
ENABLE_LETTERS_LIGATURES = 0b100
ENABLE_ALL_LIGATURES = 0b111

default_config = {
    # Supported languages are: [Arabic, ArabicV2, Kurdish]
    # More languages might be supported soon.
    # `Arabic` is default and recommended to work in most of the cases and
    # supports (Arabic, Urdu and Farsi)
    # `ArabicV2` is only to be used with certain font that you run into missing
    # chars `Kurdish` if you are using Kurdish Sarchia font is recommended,
    # work with both unicode and classic Arabic-Kurdish keybouard
    'language': 'Arabic',

    # Whether to delete the Harakat (Tashkeel) before reshaping or not.
    'delete_harakat': True,

    # Whether to shift the Harakat (Tashkeel) one position so they appear
    # correctly when string is reversed
    'shift_harakat_position': False,

    # Whether to delete the Tatweel (U+0640) before reshaping or not.
    'delete_tatweel': False,

    # Whether to support ZWJ (U+200D) or not.
    'support_zwj': True,

    # Use unshaped form instead of isolated form.
    'use_unshaped_instead_of_isolated': False,

    # Whether to use ligatures or not.
    # Serves as a shortcut to disable all ligatures.
    'support_ligatures': True,

    # When `support_ligatures` is enabled.
    # Separate ligatures configuration take precedence over it.
    # When `support_ligatures` is disabled,
    # separate ligatures configurations are ignored.

    # ------------------- Begin: Ligatures Configurations ------------------ #

    # Sentences (Enabled on top)
    'ARABIC LIGATURE BISMILLAH AR-RAHMAN AR-RAHEEM': False,
    'ARABIC LIGATURE JALLAJALALOUHOU': False,
    'ARABIC LIGATURE SALLALLAHOU ALAYHE WASALLAM': False,

    # Words (Enabled on top)
    'ARABIC LIGATURE ALLAH': True,
    'ARABIC LIGATURE AKBAR': False,
    'ARABIC LIGATURE ALAYHE': False,
    'ARABIC LIGATURE MOHAMMAD': False,
    'ARABIC LIGATURE RASOUL': False,
    'ARABIC LIGATURE SALAM': False,
    'ARABIC LIGATURE SALLA': False,
    'ARABIC LIGATURE WASALLAM': False,
    'RIAL SIGN': False,

    # Letters (Enabled on top)
    'ARABIC LIGATURE LAM WITH ALEF': True,
    'ARABIC LIGATURE LAM WITH ALEF WITH HAMZA ABOVE': True,
    'ARABIC LIGATURE LAM WITH ALEF WITH HAMZA BELOW': True,
    'ARABIC LIGATURE LAM WITH ALEF WITH MADDA ABOVE': True,
    'ARABIC LIGATURE AIN WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE AIN WITH JEEM': False,
    'ARABIC LIGATURE AIN WITH JEEM WITH MEEM': False,
    'ARABIC LIGATURE AIN WITH MEEM': False,
    'ARABIC LIGATURE AIN WITH MEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE AIN WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE AIN WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE AIN WITH YEH': False,
    'ARABIC LIGATURE ALEF MAKSURA WITH SUPERSCRIPT ALEF': False,
    'ARABIC LIGATURE ALEF WITH FATHATAN': False,
    'ARABIC LIGATURE BEH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE BEH WITH HAH': False,
    'ARABIC LIGATURE BEH WITH HAH WITH YEH': False,
    'ARABIC LIGATURE BEH WITH HEH': False,
    'ARABIC LIGATURE BEH WITH JEEM': False,
    'ARABIC LIGATURE BEH WITH KHAH': False,
    'ARABIC LIGATURE BEH WITH KHAH WITH YEH': False,
    'ARABIC LIGATURE BEH WITH MEEM': False,
    'ARABIC LIGATURE BEH WITH NOON': False,
    'ARABIC LIGATURE BEH WITH REH': False,
    'ARABIC LIGATURE BEH WITH YEH': False,
    'ARABIC LIGATURE BEH WITH ZAIN': False,
    'ARABIC LIGATURE DAD WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE DAD WITH HAH': False,
    'ARABIC LIGATURE DAD WITH HAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE DAD WITH HAH WITH YEH': False,
    'ARABIC LIGATURE DAD WITH JEEM': False,
    'ARABIC LIGATURE DAD WITH KHAH': False,
    'ARABIC LIGATURE DAD WITH KHAH WITH MEEM': False,
    'ARABIC LIGATURE DAD WITH MEEM': False,
    'ARABIC LIGATURE DAD WITH REH': False,
    'ARABIC LIGATURE DAD WITH YEH': False,
    'ARABIC LIGATURE FEH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE FEH WITH HAH': False,
    'ARABIC LIGATURE FEH WITH JEEM': False,
    'ARABIC LIGATURE FEH WITH KHAH': False,
    'ARABIC LIGATURE FEH WITH KHAH WITH MEEM': False,
    'ARABIC LIGATURE FEH WITH MEEM': False,
    'ARABIC LIGATURE FEH WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE FEH WITH YEH': False,
    'ARABIC LIGATURE GHAIN WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE GHAIN WITH JEEM': False,
    'ARABIC LIGATURE GHAIN WITH MEEM': False,
    'ARABIC LIGATURE GHAIN WITH MEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE GHAIN WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE GHAIN WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE GHAIN WITH YEH': False,
    'ARABIC LIGATURE HAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE HAH WITH JEEM': False,
    'ARABIC LIGATURE HAH WITH JEEM WITH YEH': False,
    'ARABIC LIGATURE HAH WITH MEEM': False,
    'ARABIC LIGATURE HAH WITH MEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE HAH WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE HAH WITH YEH': False,
    'ARABIC LIGATURE HEH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE HEH WITH JEEM': False,
    'ARABIC LIGATURE HEH WITH MEEM': False,
    'ARABIC LIGATURE HEH WITH MEEM WITH JEEM': False,
    'ARABIC LIGATURE HEH WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE HEH WITH SUPERSCRIPT ALEF': False,
    'ARABIC LIGATURE HEH WITH YEH': False,
    'ARABIC LIGATURE JEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE JEEM WITH HAH': False,
    'ARABIC LIGATURE JEEM WITH HAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE JEEM WITH HAH WITH YEH': False,
    'ARABIC LIGATURE JEEM WITH MEEM': False,
    'ARABIC LIGATURE JEEM WITH MEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE JEEM WITH MEEM WITH HAH': False,
    'ARABIC LIGATURE JEEM WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE JEEM WITH YEH': False,
    'ARABIC LIGATURE KAF WITH ALEF': False,
    'ARABIC LIGATURE KAF WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE KAF WITH HAH': False,
    'ARABIC LIGATURE KAF WITH JEEM': False,
    'ARABIC LIGATURE KAF WITH KHAH': False,
    'ARABIC LIGATURE KAF WITH LAM': False,
    'ARABIC LIGATURE KAF WITH MEEM': False,
    'ARABIC LIGATURE KAF WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE KAF WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE KAF WITH YEH': False,
    'ARABIC LIGATURE KHAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE KHAH WITH HAH': False,
    'ARABIC LIGATURE KHAH WITH JEEM': False,
    'ARABIC LIGATURE KHAH WITH MEEM': False,
    'ARABIC LIGATURE KHAH WITH YEH': False,
    'ARABIC LIGATURE LAM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE LAM WITH HAH': False,
    'ARABIC LIGATURE LAM WITH HAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE LAM WITH HAH WITH MEEM': False,
    'ARABIC LIGATURE LAM WITH HAH WITH YEH': False,
    'ARABIC LIGATURE LAM WITH HEH': False,
    'ARABIC LIGATURE LAM WITH JEEM': False,
    'ARABIC LIGATURE LAM WITH JEEM WITH JEEM': False,
    'ARABIC LIGATURE LAM WITH JEEM WITH MEEM': False,
    'ARABIC LIGATURE LAM WITH JEEM WITH YEH': False,
    'ARABIC LIGATURE LAM WITH KHAH': False,
    'ARABIC LIGATURE LAM WITH KHAH WITH MEEM': False,
    'ARABIC LIGATURE LAM WITH MEEM': False,
    'ARABIC LIGATURE LAM WITH MEEM WITH HAH': False,
    'ARABIC LIGATURE LAM WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE LAM WITH YEH': False,
    'ARABIC LIGATURE MEEM WITH ALEF': False,
    'ARABIC LIGATURE MEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE MEEM WITH HAH': False,
    'ARABIC LIGATURE MEEM WITH HAH WITH JEEM': False,
    'ARABIC LIGATURE MEEM WITH HAH WITH MEEM': False,
    'ARABIC LIGATURE MEEM WITH HAH WITH YEH': False,
    'ARABIC LIGATURE MEEM WITH JEEM': False,
    'ARABIC LIGATURE MEEM WITH JEEM WITH HAH': False,
    'ARABIC LIGATURE MEEM WITH JEEM WITH KHAH': False,
    'ARABIC LIGATURE MEEM WITH JEEM WITH MEEM': False,
    'ARABIC LIGATURE MEEM WITH JEEM WITH YEH': False,
    'ARABIC LIGATURE MEEM WITH KHAH': False,
    'ARABIC LIGATURE MEEM WITH KHAH WITH JEEM': False,
    'ARABIC LIGATURE MEEM WITH KHAH WITH MEEM': False,
    'ARABIC LIGATURE MEEM WITH KHAH WITH YEH': False,
    'ARABIC LIGATURE MEEM WITH MEEM': False,
    'ARABIC LIGATURE MEEM WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE MEEM WITH YEH': False,
    'ARABIC LIGATURE NOON WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE NOON WITH HAH': False,
    'ARABIC LIGATURE NOON WITH HAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE NOON WITH HAH WITH MEEM': False,
    'ARABIC LIGATURE NOON WITH HAH WITH YEH': False,
    'ARABIC LIGATURE NOON WITH HEH': False,
    'ARABIC LIGATURE NOON WITH JEEM': False,
    'ARABIC LIGATURE NOON WITH JEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE NOON WITH JEEM WITH HAH': False,
    'ARABIC LIGATURE NOON WITH JEEM WITH MEEM': False,
    'ARABIC LIGATURE NOON WITH JEEM WITH YEH': False,
    'ARABIC LIGATURE NOON WITH KHAH': False,
    'ARABIC LIGATURE NOON WITH MEEM': False,
    'ARABIC LIGATURE NOON WITH MEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE NOON WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE NOON WITH NOON': False,
    'ARABIC LIGATURE NOON WITH REH': False,
    'ARABIC LIGATURE NOON WITH YEH': False,
    'ARABIC LIGATURE NOON WITH ZAIN': False,
    'ARABIC LIGATURE QAF WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE QAF WITH HAH': False,
    'ARABIC LIGATURE QAF WITH MEEM': False,
    'ARABIC LIGATURE QAF WITH MEEM WITH HAH': False,
    'ARABIC LIGATURE QAF WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE QAF WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE QAF WITH YEH': False,
    'ARABIC LIGATURE QALA USED AS KORANIC STOP SIGN': False,
    'ARABIC LIGATURE REH WITH SUPERSCRIPT ALEF': False,
    'ARABIC LIGATURE SAD WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE SAD WITH HAH': False,
    'ARABIC LIGATURE SAD WITH HAH WITH HAH': False,
    'ARABIC LIGATURE SAD WITH HAH WITH YEH': False,
    'ARABIC LIGATURE SAD WITH KHAH': False,
    'ARABIC LIGATURE SAD WITH MEEM': False,
    'ARABIC LIGATURE SAD WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE SAD WITH REH': False,
    'ARABIC LIGATURE SAD WITH YEH': False,
    'ARABIC LIGATURE SALLA USED AS KORANIC STOP SIGN': False,
    'ARABIC LIGATURE SEEN WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE SEEN WITH HAH': False,
    'ARABIC LIGATURE SEEN WITH HAH WITH JEEM': False,
    'ARABIC LIGATURE SEEN WITH HEH': False,
    'ARABIC LIGATURE SEEN WITH JEEM': False,
    'ARABIC LIGATURE SEEN WITH JEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE SEEN WITH JEEM WITH HAH': False,
    'ARABIC LIGATURE SEEN WITH KHAH': False,
    'ARABIC LIGATURE SEEN WITH KHAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE SEEN WITH KHAH WITH YEH': False,
    'ARABIC LIGATURE SEEN WITH MEEM': False,
    'ARABIC LIGATURE SEEN WITH MEEM WITH HAH': False,
    'ARABIC LIGATURE SEEN WITH MEEM WITH JEEM': False,
    'ARABIC LIGATURE SEEN WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE SEEN WITH REH': False,
    'ARABIC LIGATURE SEEN WITH YEH': False,
    'ARABIC LIGATURE SHADDA WITH DAMMA': False,
    'ARABIC LIGATURE SHADDA WITH DAMMA ISOLATED FORM': False,
    'ARABIC LIGATURE SHADDA WITH DAMMA MEDIAL FORM': False,
    'ARABIC LIGATURE SHADDA WITH DAMMATAN ISOLATED FORM': False,
    'ARABIC LIGATURE SHADDA WITH FATHA': False,
    'ARABIC LIGATURE SHADDA WITH FATHA ISOLATED FORM': False,
    'ARABIC LIGATURE SHADDA WITH FATHA MEDIAL FORM': False,
    'ARABIC LIGATURE SHADDA WITH KASRA': False,
    'ARABIC LIGATURE SHADDA WITH KASRA ISOLATED FORM': False,
    'ARABIC LIGATURE SHADDA WITH KASRA MEDIAL FORM': False,
    'ARABIC LIGATURE SHADDA WITH KASRATAN ISOLATED FORM': False,
    'ARABIC LIGATURE SHADDA WITH SUPERSCRIPT ALEF': False,
    'ARABIC LIGATURE SHADDA WITH SUPERSCRIPT ALEF ISOLATED FORM': False,
    'ARABIC LIGATURE SHEEN WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE SHEEN WITH HAH': False,
    'ARABIC LIGATURE SHEEN WITH HAH WITH MEEM': False,
    'ARABIC LIGATURE SHEEN WITH HAH WITH YEH': False,
    'ARABIC LIGATURE SHEEN WITH HEH': False,
    'ARABIC LIGATURE SHEEN WITH JEEM': False,
    'ARABIC LIGATURE SHEEN WITH JEEM WITH YEH': False,
    'ARABIC LIGATURE SHEEN WITH KHAH': False,
    'ARABIC LIGATURE SHEEN WITH MEEM': False,
    'ARABIC LIGATURE SHEEN WITH MEEM WITH KHAH': False,
    'ARABIC LIGATURE SHEEN WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE SHEEN WITH REH': False,
    'ARABIC LIGATURE SHEEN WITH YEH': False,
    'ARABIC LIGATURE TAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE TAH WITH HAH': False,
    'ARABIC LIGATURE TAH WITH MEEM': False,
    'ARABIC LIGATURE TAH WITH MEEM WITH HAH': False,
    'ARABIC LIGATURE TAH WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE TAH WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE TAH WITH YEH': False,
    'ARABIC LIGATURE TEH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE TEH WITH HAH': False,
    'ARABIC LIGATURE TEH WITH HAH WITH JEEM': False,
    'ARABIC LIGATURE TEH WITH HAH WITH MEEM': False,
    'ARABIC LIGATURE TEH WITH HEH': False,
    'ARABIC LIGATURE TEH WITH JEEM': False,
    'ARABIC LIGATURE TEH WITH JEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE TEH WITH JEEM WITH MEEM': False,
    'ARABIC LIGATURE TEH WITH JEEM WITH YEH': False,
    'ARABIC LIGATURE TEH WITH KHAH': False,
    'ARABIC LIGATURE TEH WITH KHAH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE TEH WITH KHAH WITH MEEM': False,
    'ARABIC LIGATURE TEH WITH KHAH WITH YEH': False,
    'ARABIC LIGATURE TEH WITH MEEM': False,
    'ARABIC LIGATURE TEH WITH MEEM WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE TEH WITH MEEM WITH HAH': False,
    'ARABIC LIGATURE TEH WITH MEEM WITH JEEM': False,
    'ARABIC LIGATURE TEH WITH MEEM WITH KHAH': False,
    'ARABIC LIGATURE TEH WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE TEH WITH NOON': False,
    'ARABIC LIGATURE TEH WITH REH': False,
    'ARABIC LIGATURE TEH WITH YEH': False,
    'ARABIC LIGATURE TEH WITH ZAIN': False,
    'ARABIC LIGATURE THAL WITH SUPERSCRIPT ALEF': False,
    'ARABIC LIGATURE THEH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE THEH WITH HEH': False,
    'ARABIC LIGATURE THEH WITH JEEM': False,
    'ARABIC LIGATURE THEH WITH MEEM': False,
    'ARABIC LIGATURE THEH WITH NOON': False,
    'ARABIC LIGATURE THEH WITH REH': False,
    'ARABIC LIGATURE THEH WITH YEH': False,
    'ARABIC LIGATURE THEH WITH ZAIN': False,
    'ARABIC LIGATURE UIGHUR KIRGHIZ YEH WITH HAMZA ABOVE WITH ALEF MAKSURA': False,  # noqa
    'ARABIC LIGATURE YEH WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE YEH WITH HAH': False,
    'ARABIC LIGATURE YEH WITH HAH WITH YEH': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH AE': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH ALEF': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH ALEF MAKSURA': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH E': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH HAH': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH HEH': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH JEEM': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH KHAH': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH MEEM': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH NOON': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH OE': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH REH': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH U': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH WAW': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH YEH': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH YU': False,
    'ARABIC LIGATURE YEH WITH HAMZA ABOVE WITH ZAIN': False,
    'ARABIC LIGATURE YEH WITH HEH': False,
    'ARABIC LIGATURE YEH WITH JEEM': False,
    'ARABIC LIGATURE YEH WITH JEEM WITH YEH': False,
    'ARABIC LIGATURE YEH WITH KHAH': False,
    'ARABIC LIGATURE YEH WITH MEEM': False,
    'ARABIC LIGATURE YEH WITH MEEM WITH MEEM': False,
    'ARABIC LIGATURE YEH WITH MEEM WITH YEH': False,
    'ARABIC LIGATURE YEH WITH NOON': False,
    'ARABIC LIGATURE YEH WITH REH': False,
    'ARABIC LIGATURE YEH WITH YEH': False,
    'ARABIC LIGATURE YEH WITH ZAIN': False,
    'ARABIC LIGATURE ZAH WITH MEEM': False,
    # -------------------- End: Ligatures Configurations ------------------- #
}


def auto_config(configuration=None, configuration_file=None):
    loaded_from_envvar = False

    configuration_parser = ConfigParser()
    configuration_parser.read_dict({
        'ArabicReshaper': default_config
    })

    if not configuration_file:
        configuration_file = os.getenv(
            'PYTHON_ARABIC_RESHAPER_CONFIGURATION_FILE'
        )
        if configuration_file:
            loaded_from_envvar = True

    if configuration_file:
        if not os.path.exists(configuration_file):
            raise Exception(
                'Configuration file {} not found{}.'.format(
                    configuration_file,
                    loaded_from_envvar and (
                        ' it is set in your environment variable ' +
                        'PYTHON_ARABIC_RESHAPER_CONFIGURATION_FILE'
                    ) or ''
                )
            )
        configuration_parser.read((configuration_file,))

    if configuration:
        configuration_parser.read_dict({
            'ArabicReshaper': configuration
        })

    if 'ArabicReshaper' not in configuration_parser:
        raise ValueError(
            'Invalid configuration: '
            'A section with the name ArabicReshaper was not found'
        )

    return configuration_parser['ArabicReshaper']


def config_for_true_type_font(font_file_path,
                              ligatures_config=ENABLE_ALL_LIGATURES):
    if not with_font_config:
        raise Exception('fonttools not installed, ' +
                        'install it then rerun this.\n' +
                        '$ pip install arabic-teshaper[with-fonttools]')
    if not font_file_path or not os.path.exists(font_file_path):
        raise Exception('Invalid path to font file')
    ttfont = TTFont(font_file_path)
    has_isolated = True
    for k, v in LETTERS_ARABIC.items():
        for table in ttfont['cmap'].tables:
            if ord(v[ISOLATED]) in table.cmap:
                break
        else:
            has_isolated = False
            break

    configuration = {
        'use_unshaped_instead_of_isolated': not has_isolated,
    }

    def process_ligatures(ligatures):
        for ligature in ligatures:
            forms = list(filter(lambda form: form != '', ligature[1][1]))
            n = len(forms)
            for form in forms:
                for table in ttfont['cmap'].tables:
                    if ord(form) in table.cmap:
                        n -= 1
                        break
            configuration[ligature[0]] = (n == 0)

    if ENABLE_SENTENCES_LIGATURES & ligatures_config:
        process_ligatures(SENTENCES_LIGATURES)

    if ENABLE_WORDS_LIGATURES & ligatures_config:
        process_ligatures(WORDS_LIGATURES)

    if ENABLE_LETTERS_LIGATURES & ligatures_config:
        process_ligatures(LETTERS_LIGATURES)

    return configuration
