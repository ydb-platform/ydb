# -*- coding: utf-8 -*-
import pytest

import validators


@pytest.mark.parametrize('value', [
    (
        'cf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d'
        '13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e'
    ),
    (
        'CF83E1357EEFB8BDF1542850D66D8007D620E4050B5715DC83F4A921D36CE9CE47D0D'
        '13C5D85F2B0FF8318D2877EEC2F63B931BD47417A81A538327AF927DA3E'
    )
])
def test_returns_true_on_valid_sha512(value):
    assert validators.sha512(value)


@pytest.mark.parametrize('value', [
    (
        'zf83e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d'
        '13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e'
    ),
    (
        'cf8357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce47d0d13c'
        '5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e'
    ),
    (
        'cf8aaaa3e1357eefb8bdf1542850d66d8007d620e4050b5715dc83f4a921d36ce9ce4'
        '7d0d13c5d85f2b0ff8318d2877eec2f63b931bd47417a81a538327af927da3e'
    )
])
def test_returns_failed_validation_on_invalid_sha512(value):
    result = validators.sha512(value)
    assert isinstance(result, validators.ValidationFailure)
