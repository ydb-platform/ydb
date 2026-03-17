# -*- coding: utf-8 -*-
import pytest

from validators import email, ValidationFailure


@pytest.mark.parametrize(('value', 'whitelist'), [
    ('email@here.com', None),
    ('weirder-email@here.and.there.com', None),
    ('email@[127.0.0.1]', None),
    ('example@valid-----hyphens.com', None),
    ('example@valid-with-hyphens.com', None),
    ('test@domain.with.idn.tld.उदाहरण.परीक्षा', None),
    ('email@localhost', None),
    ('email@localdomain', ['localdomain']),
    ('"test@test"@example.com', None),
    ('"\\\011"@here.com', None),
])
def test_returns_true_on_valid_email(value, whitelist):
    assert email(value, whitelist=whitelist)


@pytest.mark.parametrize(('value',), [
    (None,),
    ('',),
    ('abc',),
    ('abc@',),
    ('abc@bar',),
    ('a @x.cz',),
    ('abc@.com',),
    ('something@@somewhere.com',),
    ('email@127.0.0.1',),
    ('example@invalid-.com',),
    ('example@-invalid.com',),
    ('example@inv-.alid-.com',),
    ('example@inv-.-alid.com',),
    # Quoted-string format (CR not allowed)
    ('"\\\012"@here.com',),
])
def test_returns_failed_validation_on_invalid_email(value):
    assert isinstance(email(value), ValidationFailure)
