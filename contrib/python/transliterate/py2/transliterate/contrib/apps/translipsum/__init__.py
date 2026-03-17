from six import PY3

from transliterate.utils import translit

if PY3:
    from transliterate.contrib.apps.translipsum.utils import Generator
else:
    from lipsum import Generator

__title__ = 'transliterate.contrib.apps.translipsum.__init__'
__author__ = 'Artur Barseghyan'
__copyright__ = '2013-2018 Artur Barseghyan'
__license__ = 'GPL 2.0/LGPL 2.1'
__all__ = ('TranslipsumGenerator',)


class TranslipsumGenerator(Generator):
    """Lorem ipsum generator."""

    def __init__(self, language_code, reversed=False, *args, **kwargs):
        self._language_code = language_code
        self._reversed = reversed
        super(TranslipsumGenerator, self).__init__(*args, **kwargs)

    def generate_sentence(self, *args, **kwargs):
        """Generate sentence."""
        value = super(TranslipsumGenerator, self).generate_sentence(
            *args, **kwargs
        )
        return translit(value,
                        language_code=self._language_code,
                        reversed=self._reversed)

    def generate_paragraph(self, *args, **kwargs):
        """Generate paragraph."""
        value = super(TranslipsumGenerator, self).generate_paragraph(
            *args, **kwargs
        )
        return translit(value,
                        language_code=self._language_code,
                        reversed=self._reversed)
