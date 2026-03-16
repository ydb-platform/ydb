
import pytest

from schematics.types import StringType, IntType
from schematics.exceptions import DataError, ValidationError
from schematics.models import Model


def test_dont_serialize_untrusted_data():
    """
    Serialization MUST ONLY contain the subset of validated data.
    """
    class Player(Model):
        code = StringType(max_length=4, default=None, serialize_when_none=True)

    p1 = Player({'code': 'invalid1'})
    assert p1.serialize() == {'code': None}
    with pytest.raises(DataError):
        p1.validate()
    assert p1.serialize() == {'code': None}


def test_dont_overwrite_with_invalid_data():
    """
    Model-level validators MAY deny data from being set.
    """
    class Player(Model):
        id = IntType()
        name = StringType()

        def validate_id(self, context, value):
            if self._data.valid.get('id'):
                raise ValidationError('Cannot change id')

    p1 = Player({'id': 4})
    p1.validate()
    p1.id = 3
    p1.name = 'Douglas'
    with pytest.raises(DataError):
        p1.validate()
    assert p1.id == 4
    assert p1.name == 'Douglas'


def test_keep_valid_data_after_multiple_validation():
    """
    Validation MUST maintain a sane state after multiple operations. Valid
    data MUST NEVER be lost or overwritten.
    """
    class Player(Model):
        id = IntType()
        code = StringType(max_length=4)

    p1 = Player({'id': 4})
    p1.validate()
    assert p1.serialize() == {'id': 4, 'code': None}
    p1.code = 'AAA'
    p1.validate()
    assert p1.serialize() == {'id': 4, 'code': 'AAA'}
    p1.code = 'BBB'
    p1.validate()
    assert p1.serialize() == {'id': 4, 'code': 'BBB'}
    p1.code = 'CCCERR'
    with pytest.raises(DataError):
        p1.validate()
    assert p1.serialize() == {'id': 4, 'code': 'BBB'}
    p1.validate()
    assert p1.serialize() == {'id': 4, 'code': 'BBB'}


def test_dont_forget_required_fields_after_multiple_validation():
    """
    Validation MUST NOT forget about required fields when
    clearing untrusted data. Model init data also MUST NOT influence
    the requirement checks.
    """
    class Player(Model):
        code = StringType(required=True)

    p1 = Player()
    with pytest.raises(DataError):
        p1.validate()
    with pytest.raises(DataError):
        p1.validate()
