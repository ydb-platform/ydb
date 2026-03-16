import platform
import sys

import pytest

from netaddr import AddrFormatError
from netaddr.strategy import ipv6


def test_strategy_ipv6():
    b = '0000000000000000:0000000000000000:0000000000000000:0000000000000000:0000000000000000:0000000000000000:1111111111111111:1111111111111110'
    i = 4294967294
    t = (0, 0, 0, 0, 0, 0, 0xffff, 0xfffe)
    s = '::255.255.255.254'

    assert ipv6.bits_to_int(b) == i
    assert ipv6.int_to_bits(i) == b

    assert ipv6.int_to_str(i) == s
    assert ipv6.str_to_int(s) == i

    assert ipv6.int_to_words(i) == t
    assert ipv6.words_to_int(t) == i
    assert ipv6.words_to_int(list(t)) == i


@pytest.mark.skipif(sys.version_info > (3,), reason="requires python 2.x")
def test_strategy_ipv6_py2():
    i = 4294967294
    p = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xfe'
    assert ipv6.int_to_packed(i) == p
    assert ipv6.packed_to_int(p) == 4294967294


@pytest.mark.skipif(sys.version_info < (3,), reason="requires python 3.x")
def test_strategy_ipv6_py3():
    i = 4294967294
    p = b'\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\xff\xff\xff\xfe'
    assert ipv6.int_to_packed(i) == p
    assert ipv6.packed_to_int(p) == 4294967294


@pytest.mark.parametrize('str_value', (
    '2001:0db8:0000:0000:0000:0000:1428:57ab',
    '2001:0db8:0000:0000:0000::1428:57ab',
    '2001:0db8:0:0:0:0:1428:57ab',
    '2001:0db8:0:0::1428:57ab',
    '2001:0db8::1428:57ab',
    '2001:0DB8:0000:0000:0000:0000:1428:57AB',
    '2001:DB8::1428:57AB',
))
def test_strategy_ipv6_equivalent_variants(str_value):
    assert ipv6.str_to_int(str_value) == 42540766411282592856903984951992014763


@pytest.mark.parametrize('str_value', (
    #   Long forms.
    'FEDC:BA98:7654:3210:FEDC:BA98:7654:3210',
    '1080:0:0:0:8:800:200C:417A',   #   a unicast address
    'FF01:0:0:0:0:0:0:43',      	#   a multicast address
    '0:0:0:0:0:0:0:1',          	#   the loopback address
    '0:0:0:0:0:0:0:0',          	#   the unspecified addresses

    #   Short forms.
    '1080::8:800:200C:417A',    	#   a unicast address
    'FF01::43',                 	#   a multicast address
    '::1',                      	#   the loopback address
    '::',                       	#   the unspecified addresses

    #   IPv4 compatible forms.
    '::192.0.2.1',
    '::ffff:192.0.2.1',
    '0:0:0:0:0:0:192.0.2.1',
    '0:0:0:0:0:FFFF:192.0.2.1',
    '0:0:0:0:0:0:13.1.68.3',
    '0:0:0:0:0:FFFF:129.144.52.38',
    '::13.1.68.3',
    '::FFFF:129.144.52.38',

    #   Other tests.
    '1::',
    '::ffff',
    'ffff::',
    'ffff::ffff',
    '0:1:2:3:4:5:6:7',
    '8:9:a:b:c:d:e:f',
    '0:0:0:0:0:0:0:0',
    'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
))
def test_strategy_ipv6_valid_str(str_value):
    assert ipv6.valid_str(str_value)


@pytest.mark.parametrize('str_value', (
    'g:h:i:j:k:l:m:n',      # bad chars.
    '0:0:0:0:0:0:0:0:0',    # too long,
    #   Unexpected types.
    [],
    (),
    {},
    True,
    False,
))
def test_strategy_ipv6_is_not_valid_str(str_value):
    assert not ipv6.valid_str(str_value)


def test_strategy_ipv6_valid_str_exception_on_empty_string():
    with pytest.raises(AddrFormatError):
        ipv6.valid_str('')


@pytest.mark.parametrize(('long_form', 'short_form'), (
    ('FEDC:BA98:7654:3210:FEDC:BA98:7654:3210', 'fedc:ba98:7654:3210:fedc:ba98:7654:3210'),
    ('1080:0:0:0:8:800:200C:417A', '1080::8:800:200c:417a'), # unicast address
    ('FF01:0:0:0:0:0:0:43', 'ff01::43'), # multicast address
    ('0:0:0:0:0:0:0:1', '::1'),          # loopback address
    ('0:0:0:0:0:0:0:0', '::'),           # unspecified addresses
))
def test_strategy_ipv6_string_compaction(long_form, short_form):
    int_val = ipv6.str_to_int(long_form)
    calc_short_form = ipv6.int_to_str(int_val)
    assert calc_short_form == short_form


def test_strategy_ipv6_mapped_and_compatible_ipv4_string_formatting():
    assert ipv6.int_to_str(0xffffff) == '::0.255.255.255'
    assert ipv6.int_to_str(0xffffffff) == '::255.255.255.255'
    assert ipv6.int_to_str(0x1ffffffff) == '::1:ffff:ffff'
    assert ipv6.int_to_str(0xffffffffffff) == '::ffff:255.255.255.255'
    assert ipv6.int_to_str(0xfffeffffffff) == '::fffe:ffff:ffff'
    assert ipv6.int_to_str(0xffffffffffff) == '::ffff:255.255.255.255'
    assert ipv6.int_to_str(0xfffffffffff1) == '::ffff:255.255.255.241'
    assert ipv6.int_to_str(0xfffffffffffe) == '::ffff:255.255.255.254'
    assert ipv6.int_to_str(0xffffffffff00) == '::ffff:255.255.255.0'
    assert ipv6.int_to_str(0xffffffff0000) == '::ffff:255.255.0.0'
    assert ipv6.int_to_str(0xffffff000000) == '::ffff:255.0.0.0'
    assert ipv6.int_to_str(0xffff000000) == '::ff:ff00:0'
    assert ipv6.int_to_str(0x1ffff00000000) == '::1:ffff:0:0'
    # So this is strange. Even though on Windows we get decimal notation in a lot of the addresses above,
    # in case of 0.0.0.0 we get hex instead, unless it's Python 2, then we get decimal... unless it's
    # actually PyPy Python 2, then we always get hex (again, only on Windows). Worth investigating, putting
    # the conditional assert here for now to make this visible.
    if platform.system() == 'Windows' and (
            platform.python_version() >= '3.0' or platform.python_implementation() == 'PyPy'
    ):
        assert ipv6.int_to_str(0xffff00000000) == '::ffff:0:0'
    else:
        assert ipv6.int_to_str(0xffff00000000) == '::ffff:0.0.0.0'


def test_strategy_ipv6_str_to_int_behaviour_legacy_mode():
    assert ipv6.str_to_int('::127') == 295

    with pytest.raises(AddrFormatError):
        ipv6.str_to_int('::0x7f')

    assert ipv6.str_to_int('::0177') == 375

    with pytest.raises(AddrFormatError):
        ipv6.str_to_int('::127.1')

    with pytest.raises(AddrFormatError):
        ipv6.str_to_int('::0x7f.1')

    with pytest.raises(AddrFormatError):
        ipv6.str_to_int('::0177.1')

    assert ipv6.str_to_int('::127.0.0.1') == 2130706433
