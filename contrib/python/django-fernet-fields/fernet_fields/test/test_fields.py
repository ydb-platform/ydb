from cryptography.fernet import Fernet
from datetime import date, datetime

from django.core.exceptions import FieldError, ImproperlyConfigured
from django.db import connection, models as dj_models
from django.utils.encoding import force_bytes, force_text
import pytest

import fernet_fields as fields
from test import models


class TestEncryptedField(object):
    def test_key_from_settings(self, settings):
        """If present, use settings.FERNET_KEYS."""
        settings.FERNET_KEYS = ['secret']
        f = fields.EncryptedTextField()

        assert f.keys == settings.FERNET_KEYS

    def test_fallback_to_secret_key(self, settings):
        """If no FERNET_KEY setting, use SECRET_KEY."""
        f = fields.EncryptedTextField()

        assert f.keys == [settings.SECRET_KEY]

    def test_key_rotation(self, settings):
        """Can supply multiple `keys` for key rotation."""
        settings.FERNET_KEYS = ['key1', 'key2']
        f = fields.EncryptedTextField()

        enc1 = Fernet(f.fernet_keys[0]).encrypt(b'enc1')
        enc2 = Fernet(f.fernet_keys[1]).encrypt(b'enc2')

        assert f.fernet.decrypt(enc1) == b'enc1'
        assert f.fernet.decrypt(enc2) == b'enc2'

    def test_no_hkdf(self, settings):
        """Can set FERNET_USE_HKDF=False to avoid HKDF."""
        settings.FERNET_USE_HKDF = False
        k1 = Fernet.generate_key()
        settings.FERNET_KEYS = [k1]
        f = fields.EncryptedTextField()
        fernet = Fernet(k1)

        assert fernet.decrypt(f.fernet.encrypt(b'foo')) == b'foo'

    @pytest.mark.parametrize('key', ['primary_key', 'db_index', 'unique'])
    def test_not_allowed(self, key):
        with pytest.raises(ImproperlyConfigured):
            fields.EncryptedIntegerField(**{key: True})

    def test_get_integer_field_validators(self):
        f = fields.EncryptedIntegerField()

        # Raises no error
        f.validators


@pytest.mark.parametrize(
    'model,vals',
    [
        (models.EncryptedText, ['foo', 'bar']),
        (models.EncryptedChar, ['one', 'two']),
        (models.EncryptedEmail, ['a@example.com', 'b@example.com']),
        (models.EncryptedInt, [1, 2]),
        (models.EncryptedDate, [date(2015, 2, 5), date(2015, 2, 8)]),
        (
            models.EncryptedDateTime,
            [datetime(2015, 2, 5, 15), datetime(2015, 2, 8, 16)],
        ),
    ],
)
class TestEncryptedFieldQueries(object):
    def test_insert(self, db, model, vals):
        """Data stored in DB is actually encrypted."""
        field = model._meta.get_field('value')
        model.objects.create(value=vals[0])
        with connection.cursor() as cur:
            cur.execute('SELECT value FROM %s' % model._meta.db_table)
            data = [
                force_text(field.fernet.decrypt(force_bytes(r[0])))
                for r in cur.fetchall()
            ]

        assert list(map(field.to_python, data)) == [vals[0]]

    def test_insert_and_select(self, db, model, vals):
        """Data round-trips through insert and select."""
        model.objects.create(value=vals[0])
        found = model.objects.get()

        assert found.value == vals[0]

    def test_update_and_select(self, db, model, vals):
        """Data round-trips through update and select."""
        model.objects.create(value=vals[0])
        model.objects.update(value=vals[1])
        found = model.objects.get()

        assert found.value == vals[1]

    def test_lookups_raise_field_error(self, db, model, vals):
        """Lookups are not allowed (they cannot succeed)."""
        model.objects.create(value=vals[0])
        field_name = model._meta.get_field('value').__class__.__name__
        lookups = set(dj_models.Field.class_lookups) - set(['isnull'])

        for lookup in lookups:
            with pytest.raises(FieldError) as exc:
                model.objects.get(**{'value__' + lookup: vals[0]})
            assert field_name in str(exc.value)
            assert lookup in str(exc.value)
            assert 'does not support lookups' in str(exc.value)


def test_nullable(db):
    """Encrypted/dual/hash field can be nullable."""
    models.EncryptedNullable.objects.create(value=None)
    found = models.EncryptedNullable.objects.get()

    assert found.value is None


def test_isnull_false_lookup(db):
    """isnull False lookup succeeds on nullable fields"""
    test_val = 3
    models.EncryptedNullable.objects.create(value=None)
    models.EncryptedNullable.objects.create(value=test_val)
    found = models.EncryptedNullable.objects.get(value__isnull=False)

    assert found.value == test_val


def test_isnull_true_lookup(db):
    """isnull True lookup succeeds on nullable fields"""
    test_val = 3
    models.EncryptedNullable.objects.create(value=None)
    models.EncryptedNullable.objects.create(value=test_val)
    found = models.EncryptedNullable.objects.get(value__isnull=True)

    assert found.value is None
