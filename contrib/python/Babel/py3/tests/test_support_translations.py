import inspect
import io
import os
import shutil
import sys
import tempfile

import pytest

from babel import support
from babel.messages import Catalog
from babel.messages.mofile import write_mo

SKIP_LGETTEXT = sys.version_info >= (3, 8)

messages1 = [
    ('foo', {'string': 'Voh'}),
    ('foo', {'string': 'VohCTX', 'context': 'foo'}),
    (('foo1', 'foos1'), {'string': ('Voh1', 'Vohs1')}),
    (('foo1', 'foos1'), {'string': ('VohCTX1', 'VohsCTX1'), 'context': 'foo'}),
]

messages2 = [
    ('foo', {'string': 'VohD'}),
    ('foo', {'string': 'VohCTXD', 'context': 'foo'}),
    (('foo1', 'foos1'), {'string': ('VohD1', 'VohsD1')}),
    (('foo1', 'foos1'), {'string': ('VohCTXD1', 'VohsCTXD1'), 'context': 'foo'}),
]


@pytest.fixture(autouse=True)
def use_en_us_locale(monkeypatch):
    # Use a locale which won't fail to run the tests
    monkeypatch.setenv('LANG', 'en_US.UTF-8')


@pytest.fixture()
def translations() -> support.Translations:
    catalog1 = Catalog(locale='en_GB', domain='messages')
    catalog2 = Catalog(locale='en_GB', domain='messages1')
    for ids, kwargs in messages1:
        catalog1.add(ids, **kwargs)
    for ids, kwargs in messages2:
        catalog2.add(ids, **kwargs)
    catalog1_fp = io.BytesIO()
    catalog2_fp = io.BytesIO()
    write_mo(catalog1_fp, catalog1)
    catalog1_fp.seek(0)
    write_mo(catalog2_fp, catalog2)
    catalog2_fp.seek(0)
    translations1 = support.Translations(catalog1_fp)
    translations2 = support.Translations(catalog2_fp, domain='messages1')
    return translations1.add(translations2, merge=False)


@pytest.fixture(scope='module')
def empty_translations() -> support.Translations:
    fp = io.BytesIO()
    write_mo(fp, Catalog(locale='de'))
    fp.seek(0)
    return support.Translations(fp=fp)


@pytest.fixture(scope='module')
def null_translations() -> support.NullTranslations:
    fp = io.BytesIO()
    write_mo(fp, Catalog(locale='de'))
    fp.seek(0)
    return support.NullTranslations(fp=fp)


def assert_equal_type_too(expected, result) -> None:
    assert expected == result
    assert type(expected) is type(result), (
        f'instance types do not match: {type(expected)!r}!={type(result)!r}'
    )


def test_pgettext(translations):
    assert_equal_type_too('Voh', translations.gettext('foo'))
    assert_equal_type_too('VohCTX', translations.pgettext('foo', 'foo'))
    assert_equal_type_too('VohCTX1', translations.pgettext('foo', 'foo1'))


def test_pgettext_fallback(translations):
    fallback = translations._fallback
    translations._fallback = support.NullTranslations()
    assert translations.pgettext('foo', 'bar') == 'bar'
    translations._fallback = fallback


def test_upgettext(translations):
    assert_equal_type_too('Voh', translations.ugettext('foo'))
    assert_equal_type_too('VohCTX', translations.upgettext('foo', 'foo'))


@pytest.mark.skipif(SKIP_LGETTEXT, reason='lgettext is deprecated')
def test_lpgettext(translations):
    assert_equal_type_too(b'Voh', translations.lgettext('foo'))
    assert_equal_type_too(b'VohCTX', translations.lpgettext('foo', 'foo'))


def test_npgettext(translations):
    assert_equal_type_too('Voh1', translations.ngettext('foo1', 'foos1', 1))
    assert_equal_type_too('Vohs1', translations.ngettext('foo1', 'foos1', 2))
    assert_equal_type_too('VohCTX1', translations.npgettext('foo', 'foo1', 'foos1', 1))
    assert_equal_type_too('VohsCTX1', translations.npgettext('foo', 'foo1', 'foos1', 2))


def test_unpgettext(translations):
    assert_equal_type_too('Voh1', translations.ungettext('foo1', 'foos1', 1))
    assert_equal_type_too('Vohs1', translations.ungettext('foo1', 'foos1', 2))
    assert_equal_type_too('VohCTX1', translations.unpgettext('foo', 'foo1', 'foos1', 1))
    assert_equal_type_too('VohsCTX1', translations.unpgettext('foo', 'foo1', 'foos1', 2))


@pytest.mark.skipif(SKIP_LGETTEXT, reason='lgettext is deprecated')
def test_lnpgettext(translations):
    assert_equal_type_too(b'Voh1', translations.lngettext('foo1', 'foos1', 1))
    assert_equal_type_too(b'Vohs1', translations.lngettext('foo1', 'foos1', 2))
    assert_equal_type_too(b'VohCTX1', translations.lnpgettext('foo', 'foo1', 'foos1', 1))
    assert_equal_type_too(b'VohsCTX1', translations.lnpgettext('foo', 'foo1', 'foos1', 2))


def test_dpgettext(translations):
    assert_equal_type_too('VohD', translations.dgettext('messages1', 'foo'))
    assert_equal_type_too('VohCTXD', translations.dpgettext('messages1', 'foo', 'foo'))


def test_dupgettext(translations):
    assert_equal_type_too('VohD', translations.dugettext('messages1', 'foo'))
    assert_equal_type_too('VohCTXD', translations.dupgettext('messages1', 'foo', 'foo'))


@pytest.mark.skipif(SKIP_LGETTEXT, reason='lgettext is deprecated')
def test_ldpgettext(translations):
    assert_equal_type_too(b'VohD', translations.ldgettext('messages1', 'foo'))
    assert_equal_type_too(b'VohCTXD', translations.ldpgettext('messages1', 'foo', 'foo'))


def test_dnpgettext(translations):
    assert_equal_type_too('VohD1', translations.dngettext('messages1', 'foo1', 'foos1', 1))
    assert_equal_type_too('VohsD1', translations.dngettext('messages1', 'foo1', 'foos1', 2))
    assert_equal_type_too('VohCTXD1', translations.dnpgettext('messages1', 'foo', 'foo1', 'foos1', 1))
    assert_equal_type_too('VohsCTXD1', translations.dnpgettext('messages1', 'foo', 'foo1', 'foos1', 2))


def test_dunpgettext(translations):
    assert_equal_type_too('VohD1', translations.dungettext('messages1', 'foo1', 'foos1', 1))
    assert_equal_type_too('VohsD1', translations.dungettext('messages1', 'foo1', 'foos1', 2))
    assert_equal_type_too('VohCTXD1', translations.dunpgettext('messages1', 'foo', 'foo1', 'foos1', 1))
    assert_equal_type_too('VohsCTXD1', translations.dunpgettext('messages1', 'foo', 'foo1', 'foos1', 2))


@pytest.mark.skipif(SKIP_LGETTEXT, reason='lgettext is deprecated')
def test_ldnpgettext(translations):
    assert_equal_type_too(b'VohD1', translations.ldngettext('messages1', 'foo1', 'foos1', 1))
    assert_equal_type_too(b'VohsD1', translations.ldngettext('messages1', 'foo1', 'foos1', 2))
    assert_equal_type_too(b'VohCTXD1', translations.ldnpgettext('messages1', 'foo', 'foo1', 'foos1', 1))
    assert_equal_type_too(b'VohsCTXD1', translations.ldnpgettext('messages1', 'foo', 'foo1', 'foos1', 2))


def test_load(translations):
    tempdir = tempfile.mkdtemp()
    try:
        messages_dir = os.path.join(tempdir, 'fr', 'LC_MESSAGES')
        os.makedirs(messages_dir)
        catalog = Catalog(locale='fr', domain='messages')
        catalog.add('foo', 'bar')
        with open(os.path.join(messages_dir, 'messages.mo'), 'wb') as f:
            write_mo(f, catalog)

        translations = support.Translations.load(tempdir, locales=('fr',), domain='messages')
        assert translations.gettext('foo') == 'bar'
    finally:
        shutil.rmtree(tempdir)


def get_gettext_method_names(obj):
    names = [name for name in dir(obj) if 'gettext' in name]
    if SKIP_LGETTEXT:
        # Remove deprecated l*gettext functions
        names = [name for name in names if not name.startswith('l')]
    return names


def test_null_translations_have_same_methods(empty_translations, null_translations):
    for name in get_gettext_method_names(empty_translations):
        assert hasattr(null_translations, name), f'NullTranslations does not provide method {name!r}'


def test_null_translations_method_signature_compatibility(empty_translations, null_translations):
    for name in get_gettext_method_names(empty_translations):
        assert (
            inspect.getfullargspec(getattr(empty_translations, name)) ==
            inspect.getfullargspec(getattr(null_translations, name))
        )


def test_null_translations_same_return_values(empty_translations, null_translations):
    data = {
        'message': 'foo',
        'domain': 'domain',
        'context': 'tests',
        'singular': 'bar',
        'plural': 'baz',
        'num': 1,
        'msgid1': 'bar',
        'msgid2': 'baz',
        'n': 1,
    }
    for name in get_gettext_method_names(empty_translations):
        method = getattr(empty_translations, name)
        null_method = getattr(null_translations, name)
        signature = inspect.getfullargspec(method)
        parameter_names = [name for name in signature.args if name != 'self']
        values = [data[name] for name in parameter_names]
        assert method(*values) == null_method(*values)


def test_catalog_merge_files():
    # Refs issues #92, #162
    t1 = support.Translations()
    assert t1.files == []
    t1._catalog['foo'] = 'bar'
    fp = io.BytesIO()
    write_mo(fp, Catalog())
    fp.seek(0)
    fp.name = 'pro.mo'
    t2 = support.Translations(fp)
    assert t2.files == ['pro.mo']
    t2._catalog['bar'] = 'quux'
    t1.merge(t2)
    assert t1.files == ['pro.mo']
    assert set(t1._catalog.keys()) == {'', 'foo', 'bar'}
