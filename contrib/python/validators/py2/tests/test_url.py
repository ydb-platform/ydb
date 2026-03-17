# -*- coding: utf-8 -*-
import pytest

from validators import url, ValidationFailure


@pytest.mark.parametrize('address', [
    u'http://foobar.dk',
    u'http://foobar.museum/foobar',
    u'http://fo.com',
    u'http://FOO.com',
    u'http://foo.com/blah_blah',
    u'http://foo.com/blah_blah/',
    u'http://foo.com/blah_blah_(wikipedia)',
    u'http://foo.com/blah_blah_(wikipedia)_(again)',
    u'http://www.example.com/wpstyle/?p=364',
    u'https://www.example.com/foo/?bar=baz&inga=42&quux',
    u'https://www.example.com?bar=baz',
    u'http://✪df.ws/123',
    u'http://userid:password@example.com:8080',
    u'http://userid:password@example.com:8080/',
    u'http://userid@example.com',
    u'http://userid@example.com/',
    u'http://userid@example.com:8080',
    u'http://userid@example.com:8080/',
    u'http://userid:password@example.com',
    u'http://userid:password@example.com/',
    u'http://142.42.1.1/',
    u'http://142.42.1.1:8080/',
    u'http://➡.ws/䨹',
    u'http://⌘.ws',
    u'http://⌘.ws/',
    u'http://foo.com/blah_(wikipedia)#cite-1',
    u'http://foo.com/blah_(wikipedia)_blah#cite-1',
    u'http://foo.com/unicode_(✪)_in_parens',
    u'http://foo.com/(something)?after=parens',
    u'http://☺.damowmow.com/',
    u'http://code.google.com/events/#&product=browser',
    u'http://j.mp',
    u'ftp://foo.bar/baz',
    u'http://foo.bar/?q=Test%20URL-encoded%20stuff',
    u'http://مثال.إختبار',
    u'http://例子.测试',
    u'http://उदाहरण.परीक्षा',
    u'http://-.~_!$&\'()*+,;=:%40:80%2f::::::@example.com',
    u'http://1337.net',
    u'http://a.b-c.de',
    u'http://223.255.255.254',
    u'http://10.1.1.1',
    u'http://10.1.1.254',
    u'http://127.0.0.1:8080',
    u'http://127.0.10.150',
    u'http://localhost',
    u'http://localhost:8000',
    u'http://[FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:80/index.html',
    u'http://[1080:0:0:0:8:800:200C:417A]/index.html',
    u'http://[3ffe:2a00:100:7031::1]',
    u'http://[1080::8:800:200C:417A]/foo',
    u'http://[::192.9.5.5]/ipng',
    u'http://[::FFFF:129.144.52.38]:80/index.html',
    u'http://[2010:836B:4179::836B:4179]',
])
def test_returns_true_on_valid_url(address):
    assert url(address)


@pytest.mark.parametrize('address, public', [
    (u'http://foo.bar', True),
    (u'http://username:password@example.com:4010/', False),
    (u'http://username:password@112.168.10.10:4010/', True),
    (u'http://username:password@192.168.10.10:4010/', False),
    (u'http://10.0.10.1', False),
    (u'http://127.0.0.1', False),
])
def test_returns_true_on_valid_public_url(address, public):
    assert url(address, public=public)


@pytest.mark.parametrize('address', [
    'http://foobar',
    'foobar.dk',
    'http://127.0.0/asdf',
    'http://foobar.d',
    'http://foobar.12',
    'http://foobar',
    'htp://foobar.com',
    'http://foobar..com',
    'http://fo..com',
    'http://',
    'http://.',
    'http://..',
    'http://../',
    'http://?',
    'http://??',
    'http://??/',
    'http://#',
    'http://##',
    'http://##/',
    'http://foo.bar?q=Spaces should be encoded',
    '//',
    '//a',
    '///a',
    '///',
    'http:///a',
    'foo.com',
    'rdar://1234',
    'h://test',
    'http:// shouldfail.com',
    ':// should fail',
    'http://foo.bar/foo(bar)baz quux',
    'ftps://foo.bar/',
    'http://-error-.invalid/',
    'http://a.b--c.de/',
    'http://-a.b.co',
    'http://a.b-.co',
    'http://0.0.0.0',
    'http://10.1.1.0',
    'http://10.1.1.255',
    'http://224.1.1.1',
    'http://1.1.1.1.1',
    'http://123.123.123',
    'http://3628126748',
    'http://.www.foo.bar/',
    'http://www.foo.bar./',
    'http://.www.foo.bar./',
    'http://127.12.0.260',
    'http://example.com/">user@example.com',
    'http://[2010:836B:4179::836B:4179',
    'http://2010:836B:4179::836B:4179',
    'http://2010:836B:4179::836B:4179:80/index.html',
])
def test_returns_failed_validation_on_invalid_url(address):
    assert isinstance(url(address), ValidationFailure)


@pytest.mark.parametrize('address, public', [
    (u'http://username:password@192.168.10.10:4010/', True),
    (u'http://10.0.10.1', True),
    (u'http://127.0.0.1', True),
    (u'foo://127.0.0.1', True),
    (u'http://username:password@127.0.0.1:8080', True),
    (u'http://localhost', True),
    (u'http://localhost:8000', True),

])
def test_returns_failed_validation_on_invalid_public_url(address, public):
    assert isinstance(url(address, public=public), ValidationFailure)
