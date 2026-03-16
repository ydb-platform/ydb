import pytest

try:
    from bson.objectid import ObjectId
except ImportError:
    ObjectId = None
else:
    from schematics.contrib.mongo import ObjectIdType
    from schematics.exceptions import ConversionError, ValidationError

    FAKE_OID = ObjectId()

pytestmark = pytest.mark.skipif(ObjectId is None,
                                reason='requires pymongo')


def test_to_native():
    oid = ObjectIdType()

    assert oid.to_native(FAKE_OID) == FAKE_OID
    assert oid.to_native(str(FAKE_OID)) == FAKE_OID

    with pytest.raises(ConversionError):
        oid.to_native('foo')


def test_to_primitive():
    oid = ObjectIdType()

    assert oid.to_primitive(FAKE_OID) == str(FAKE_OID)
    assert oid.to_primitive(str(FAKE_OID)) == str(FAKE_OID)


def test_validate_id():
    oid = ObjectIdType()

    oid.validate(FAKE_OID)
    oid.validate(str(FAKE_OID))

    with pytest.raises(ConversionError):
        oid.validate('foo')
