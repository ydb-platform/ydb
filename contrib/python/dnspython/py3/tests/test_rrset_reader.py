import pytest

import dns.rrset
from dns.zonefile import read_rrsets

expected_mx_1= dns.rrset.from_text('name.', 300, 'in', 'mx', '10 a.', '20 b.')
expected_mx_2 = dns.rrset.from_text('name.', 10, 'in', 'mx', '10 a.', '20 b.')
expected_mx_3 = dns.rrset.from_text('foo.', 10, 'in', 'mx', '10 a.')
expected_mx_4 = dns.rrset.from_text('bar.', 10, 'in', 'mx', '20 b.')
expected_mx_5 = dns.rrset.from_text('foo.example.', 10, 'in', 'mx',
                                    '10 a.example.')
expected_mx_6 = dns.rrset.from_text('bar.example.', 10, 'in', 'mx', '20 b.')
expected_mx_7 = dns.rrset.from_text('foo', 10, 'in', 'mx', '10 a')
expected_mx_8 = dns.rrset.from_text('bar', 10, 'in', 'mx', '20 b.')
expected_ns_1 = dns.rrset.from_text('name.', 300, 'in', 'ns', 'hi.')
expected_ns_2 = dns.rrset.from_text('name.', 300, 'ch', 'ns', 'hi.')

def equal_rrsets(a, b):
    # return True iff. a and b have the same rrsets regardless of order
    if len(a) != len(b):
        return False
    for rrset in a:
        if not rrset in b:
            return False
    return True

def test_name_ttl_rdclass_forced():
    input=''';
mx 10 a
mx 20 b.
ns hi'''
    rrsets = read_rrsets(input, name='name', ttl=300)
    assert equal_rrsets(rrsets, [expected_mx_1, expected_ns_1])
    assert rrsets[0].ttl == 300
    assert rrsets[1].ttl == 300

def test_name_ttl_rdclass_forced_rdata_split():
    input=''';
mx 10 a
ns hi
mx 20 b.'''
    rrsets = read_rrsets(input, name='name', ttl=300)
    assert equal_rrsets(rrsets, [expected_mx_1, expected_ns_1])

def test_name_ttl_rdclass_rdtype_forced():
    input=''';
10 a
20 b.'''
    rrsets = read_rrsets(input, name='name', ttl=300, rdtype='mx')
    assert equal_rrsets(rrsets, [expected_mx_1])

def test_name_rdclass_forced():
    input = '''30 mx 10 a
10 mx 20 b.
'''
    rrsets = read_rrsets(input, name='name')
    assert equal_rrsets(rrsets, [expected_mx_2])
    assert rrsets[0].ttl == 10

def test_rdclass_forced():
    input = ''';
foo 20 mx 10 a
bar 30 mx 20 b.
'''
    rrsets = read_rrsets(input)
    assert equal_rrsets(rrsets, [expected_mx_3, expected_mx_4])

def test_rdclass_forced_with_origin():
    input = ''';
foo 20 mx 10 a
bar.example. 30 mx 20 b.
'''
    rrsets = read_rrsets(input, origin='example')
    assert equal_rrsets(rrsets, [expected_mx_5, expected_mx_6])


def test_rdclass_forced_with_origin_relativized():
    input = ''';
foo 20 mx 10 a.example.
bar.example. 30 mx 20 b.
'''
    rrsets = read_rrsets(input, origin='example', relativize=True)
    assert equal_rrsets(rrsets, [expected_mx_7, expected_mx_8])

def test_rdclass_matching_default_tolerated():
    input = ''';
foo 20 mx 10 a.example.
bar.example. 30 in mx 20 b.
'''
    rrsets = read_rrsets(input, origin='example', relativize=True,
                         rdclass=None)
    assert equal_rrsets(rrsets, [expected_mx_7, expected_mx_8])

def test_rdclass_not_matching_default_rejected():
    input = ''';
foo 20 mx 10 a.example.
bar.example. 30 ch mx 20 b.
'''
    with pytest.raises(dns.exception.SyntaxError):
        rrsets = read_rrsets(input, origin='example', relativize=True,
                             rdclass=None)

def test_default_rdclass_is_none():
    input = ''
    with pytest.raises(TypeError):
        rrsets = read_rrsets(input, default_rdclass=None, origin='example',
                             relativize=True)

def test_name_rdclass_rdtype_force():
    # No real-world usage should do this, but it can be specified so we test it.
    input = ''';
30 10 a
10 20 b.
'''
    rrsets = read_rrsets(input, name='name', rdtype='mx')
    assert equal_rrsets(rrsets, [expected_mx_1])
    assert rrsets[0].ttl == 10

def test_rdclass_rdtype_force():
    # No real-world usage should do this, but it can be specified so we test it.
    input = ''';
foo 30 10 a
bar 30 20 b.
'''
    rrsets = read_rrsets(input, rdtype='mx')
    assert equal_rrsets(rrsets, [expected_mx_3, expected_mx_4])

# also weird but legal
#input5 = '''foo 30 10 a
#bar 10 20 foo.
#'''
