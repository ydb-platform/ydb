__title__ = 'transliterate.defaults'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = (
    'CONTRIB_DIR',
    'DEBUG',
    'LANGUAGE_DETECTION_MAX_NUM_KEYWORDS',
    'LANGUAGE_PACK_MODULE_NAME',
    'LANGUAGES_DIR',
)

LANGUAGES_DIR = ('contrib', 'languages')

CONTRIB_DIR = ('contrib', 'apps')
LANGUAGE_PACK_MODULE_NAME = 'translit_language_pack'
LANGUAGE_DETECTION_MAX_NUM_KEYWORDS = 16

DEBUG = False
