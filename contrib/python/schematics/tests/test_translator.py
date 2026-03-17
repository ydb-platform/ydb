# -*- coding: utf-8 -*-

import pytest


def test_translator():
    def translator(string):
        translations = {'String value is too long.': 'Tamanho de texto muito grande.'}
        return translations.get(string, string)

    from schematics.translator import register_translator
    register_translator(translator)

    from schematics.types import StringType
    from schematics.exceptions import ValidationError
    with pytest.raises(ValidationError) as exc:
        StringType(max_length=1).validate_length('Abc')
    assert exc.value == ['Tamanho de texto muito grande.']
