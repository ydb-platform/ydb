# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import asyncio

import pytest

import dns.asyncbackend
import dns.asyncquery
import dns.message
import dns.query
import dns.tsigkeyring
import dns.versioned
import dns.xfr

# Some tests use a "nano nameserver" for testing.  It requires trio
# and threading, so try to import it and if it doesn't work, skip
# those tests.
try:
    from .nanonameserver import Server
    _nanonameserver_available = True
except ImportError:
    _nanonameserver_available = False
    class Server(object):
        pass

axfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN AXFR
;ANSWER
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
@ 3600 IN SOA foo bar 1 2 3 4 5
'''

axfr1 = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN AXFR
;ANSWER
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
'''
axfr2 = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;ANSWER
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
@ 3600 IN SOA foo bar 1 2 3 4 5
'''

base = """@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
"""

axfr_unexpected_origin = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN AXFR
;ANSWER
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN SOA foo bar 1 2 3 4 7
'''

ixfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN SOA foo bar 1 2 3 4 5
bar.foo 300 IN MX 0 blaz.foo
ns2 3600 IN A 10.0.0.2
@ 3600 IN SOA foo bar 2 2 3 4 5
ns2 3600 IN A 10.0.0.4
@ 3600 IN SOA foo bar 2 2 3 4 5
@ 3600 IN SOA foo bar 3 2 3 4 5
ns3 3600 IN A 10.0.0.3
@ 3600 IN SOA foo bar 3 2 3 4 5
@ 3600 IN NS ns2
@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN SOA foo bar 4 2 3 4 5
'''

compressed_ixfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN SOA foo bar 1 2 3 4 5
bar.foo 300 IN MX 0 blaz.foo
ns2 3600 IN A 10.0.0.2
@ 3600 IN NS ns2
@ 3600 IN SOA foo bar 4 2 3 4 5
ns2 3600 IN A 10.0.0.4
ns3 3600 IN A 10.0.0.3
@ 3600 IN SOA foo bar 4 2 3 4 5
'''

ixfr_expected = """@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN NS ns1
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.4
ns3 3600 IN A 10.0.0.3
"""

ixfr_first_message = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 4 2 3 4 5
'''

ixfr_header = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;ANSWER
'''

ixfr_body = [
    '@ 3600 IN SOA foo bar 1 2 3 4 5',
    'bar.foo 300 IN MX 0 blaz.foo',
    'ns2 3600 IN A 10.0.0.2',
    '@ 3600 IN SOA foo bar 2 2 3 4 5',
    'ns2 3600 IN A 10.0.0.4',
    '@ 3600 IN SOA foo bar 2 2 3 4 5',
    '@ 3600 IN SOA foo bar 3 2 3 4 5',
    'ns3 3600 IN A 10.0.0.3',
    '@ 3600 IN SOA foo bar 3 2 3 4 5',
    '@ 3600 IN NS ns2',
    '@ 3600 IN SOA foo bar 4 2 3 4 5',
    '@ 3600 IN SOA foo bar 4 2 3 4 5',
]

ixfrs = [ixfr_first_message]
ixfrs.extend([ixfr_header + l for l in ixfr_body])

good_empty_ixfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 1 2 3 4 5
'''

retry_tcp_ixfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 5 2 3 4 5
'''

bad_empty_ixfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN SOA foo bar 4 2 3 4 5
'''

unexpected_end_ixfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN SOA foo bar 1 2 3 4 5
bar.foo 300 IN MX 0 blaz.foo
ns2 3600 IN A 10.0.0.2
@ 3600 IN NS ns2
@ 3600 IN SOA foo bar 3 2 3 4 5
ns2 3600 IN A 10.0.0.4
ns3 3600 IN A 10.0.0.3
@ 3600 IN SOA foo bar 4 2 3 4 5
'''

unexpected_end_ixfr_2 = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN SOA foo bar 1 2 3 4 5
bar.foo 300 IN MX 0 blaz.foo
ns2 3600 IN A 10.0.0.2
@ 3600 IN NS ns2
'''

bad_serial_ixfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 4 2 3 4 5
@ 3600 IN SOA foo bar 2 2 3 4 5
bar.foo 300 IN MX 0 blaz.foo
ns2 3600 IN A 10.0.0.2
@ 3600 IN NS ns2
@ 3600 IN SOA foo bar 4 2 3 4 5
ns2 3600 IN A 10.0.0.4
ns3 3600 IN A 10.0.0.3
@ 3600 IN SOA foo bar 4 2 3 4 5
'''

ixfr_axfr = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
@ 3600 IN SOA foo bar 1 2 3 4 5
'''

def test_basic_axfr():
    z = dns.versioned.Zone('example.')
    m = dns.message.from_text(axfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.AXFR) as xfr:
        done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(base, 'example.')
    assert z == ez

def test_basic_axfr_unversioned():
    z = dns.zone.Zone('example.')
    m = dns.message.from_text(axfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.AXFR) as xfr:
        done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(base, 'example.')
    assert z == ez

def test_basic_axfr_two_parts():
    z = dns.versioned.Zone('example.')
    m1 = dns.message.from_text(axfr1, origin=z.origin,
                               one_rr_per_rrset=True)
    m2 = dns.message.from_text(axfr2, origin=z.origin,
                               one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.AXFR) as xfr:
        done = xfr.process_message(m1)
        assert not done
        done = xfr.process_message(m2)
        assert done
    ez = dns.zone.from_text(base, 'example.')
    assert z == ez

def test_axfr_unexpected_origin():
    z = dns.versioned.Zone('example.')
    m = dns.message.from_text(axfr_unexpected_origin, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.AXFR) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_basic_ixfr():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(ixfr_expected, 'example.')
    assert z == ez

def test_basic_ixfr_unversioned():
    z = dns.zone.from_text(base, 'example.')
    m = dns.message.from_text(ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(ixfr_expected, 'example.')
    assert z == ez

def test_compressed_ixfr():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(compressed_ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(ixfr_expected, 'example.')
    assert z == ez

def test_basic_ixfr_many_parts():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        done = False
        for text in ixfrs:
            assert not done
            m = dns.message.from_text(text, origin=z.origin,
                                      one_rr_per_rrset=True)
            done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(ixfr_expected, 'example.')
    assert z == ez

def test_good_empty_ixfr():
    z = dns.zone.from_text(ixfr_expected, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(good_empty_ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(ixfr_expected, 'example.')
    assert z == ez

def test_retry_tcp_ixfr():
    z = dns.zone.from_text(ixfr_expected, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(retry_tcp_ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1, is_udp=True) as xfr:
        with pytest.raises(dns.xfr.UseTCP):
            xfr.process_message(m)

def test_bad_empty_ixfr():
    z = dns.zone.from_text(ixfr_expected, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(bad_empty_ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=3) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_serial_went_backwards_ixfr():
    z = dns.zone.from_text(ixfr_expected, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(bad_empty_ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=5) as xfr:
        with pytest.raises(dns.xfr.SerialWentBackwards):
            xfr.process_message(m)

def test_ixfr_is_axfr():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(ixfr_axfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=0xffffffff) as xfr:
        done = xfr.process_message(m)
        assert done
    ez = dns.zone.from_text(base, 'example.')
    assert z == ez

def test_ixfr_requires_serial():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    with pytest.raises(ValueError):
        dns.xfr.Inbound(z, dns.rdatatype.IXFR)

def test_ixfr_unexpected_end_bad_diff_sequence():
    # This is where we get the end serial, but haven't seen all of
    # the expected diffs
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(unexpected_end_ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_udp_ixfr_unexpected_end_just_stops():
    # This is where everything looks good, but the IXFR just stops
    # in the middle.
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(unexpected_end_ixfr_2, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1, is_udp=True) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_ixfr_bad_serial():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(bad_serial_ixfr, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_no_udp_with_axfr():
    z = dns.versioned.Zone('example.')
    with pytest.raises(ValueError):
        with dns.xfr.Inbound(z, dns.rdatatype.AXFR, is_udp=True) as xfr:
            pass

refused = '''id 1
opcode QUERY
rcode REFUSED
flags AA
;QUESTION
example. IN AXFR
'''

bad_qname = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
not-example. IN IXFR
'''

bad_qtype = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN AXFR
'''

soa_not_first = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
bar.foo 300 IN MX 0 blaz.foo
'''

soa_not_first_2 = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ANSWER
@ 300 IN MX 0 blaz.foo
'''

no_answer = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN IXFR
;ADDITIONAL
bar.foo 300 IN MX 0 blaz.foo
'''

axfr_answers_after_final_soa = '''id 1
opcode QUERY
rcode NOERROR
flags AA
;QUESTION
example. IN AXFR
;ANSWER
@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
bar.foo 300 IN MX 0 blaz.foo
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
@ 3600 IN SOA foo bar 1 2 3 4 5
ns3 3600 IN A 10.0.0.3
'''

def test_refused():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(refused, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.xfr.TransferError):
            xfr.process_message(m)

def test_bad_qname():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(bad_qname, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_bad_qtype():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(bad_qtype, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_soa_not_first():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(soa_not_first, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)
    m = dns.message.from_text(soa_not_first_2, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_no_answer():
    z = dns.zone.from_text(base, 'example.',
                           zone_factory=dns.versioned.Zone)
    m = dns.message.from_text(no_answer, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.IXFR, serial=1) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

def test_axfr_answers_after_final_soa():
    z = dns.versioned.Zone('example.')
    m = dns.message.from_text(axfr_answers_after_final_soa, origin=z.origin,
                              one_rr_per_rrset=True)
    with dns.xfr.Inbound(z, dns.rdatatype.AXFR) as xfr:
        with pytest.raises(dns.exception.FormError):
            xfr.process_message(m)

keyring = dns.tsigkeyring.from_text(
    {
        'keyname.': 'NjHwPsMKjdN++dOfE5iAiQ=='
    }
)

keyname = dns.name.from_text('keyname')

def test_make_query_basic():
    z = dns.versioned.Zone('example.')
    (q, s) = dns.xfr.make_query(z)
    assert q.question[0].rdtype == dns.rdatatype.AXFR
    assert s is None
    (q, s) = dns.xfr.make_query(z, serial=None)
    assert q.question[0].rdtype == dns.rdatatype.AXFR
    assert s is None
    (q, s) = dns.xfr.make_query(z, serial=10)
    assert q.question[0].rdtype == dns.rdatatype.IXFR
    assert q.authority[0].rdtype == dns.rdatatype.SOA
    assert q.authority[0][0].serial == 10
    assert s == 10
    with z.writer() as txn:
        txn.add('@', 300, dns.rdata.from_text('in', 'soa', '. . 1 2 3 4 5'))
    (q, s) = dns.xfr.make_query(z)
    assert q.question[0].rdtype == dns.rdatatype.IXFR
    assert q.authority[0].rdtype == dns.rdatatype.SOA
    assert q.authority[0][0].serial == 1
    assert s == 1
    (q, s) = dns.xfr.make_query(z, keyring=keyring, keyname=keyname)
    assert q.question[0].rdtype == dns.rdatatype.IXFR
    assert q.authority[0].rdtype == dns.rdatatype.SOA
    assert q.authority[0][0].serial == 1
    assert s == 1
    assert q.keyname == keyname


def test_make_query_bad_serial():
    z = dns.versioned.Zone('example.')
    with pytest.raises(ValueError):
        dns.xfr.make_query(z, serial='hi')
    with pytest.raises(ValueError):
        dns.xfr.make_query(z, serial=-1)
    with pytest.raises(ValueError):
        dns.xfr.make_query(z, serial=4294967296)

def test_extract_serial_from_query():
    z = dns.versioned.Zone('example.')
    (q, s) = dns.xfr.make_query(z)
    xs = dns.xfr.extract_serial_from_query(q)
    assert s is None
    assert s == xs
    (q, s) = dns.xfr.make_query(z, serial=10)
    xs = dns.xfr.extract_serial_from_query(q)
    assert s == 10
    assert s == xs
    q = dns.message.make_query('example', 'a')
    with pytest.raises(ValueError):
        dns.xfr.extract_serial_from_query(q)


class XFRNanoNameserver(Server):

    def __init__(self):
        super().__init__(origin=dns.name.from_text('example'))

    def handle(self, request):
        try:
            if request.message.question[0].rdtype == dns.rdatatype.IXFR:
                text = ixfr
            else:
                text = axfr
            r = dns.message.from_text(text, one_rr_per_rrset=True,
                                      origin=self.origin)
            r.id = request.message.id
            return r
        except Exception:
            pass

@pytest.mark.skipif(not _nanonameserver_available,
                    reason="requires nanonameserver")
def test_sync_inbound_xfr():
    with XFRNanoNameserver() as ns:
        zone = dns.versioned.Zone('example')
        dns.query.inbound_xfr(ns.tcp_address[0], zone, port=ns.tcp_address[1],
                              udp_mode=dns.query.UDPMode.TRY_FIRST)
        dns.query.inbound_xfr(ns.tcp_address[0], zone, port=ns.tcp_address[1],
                              udp_mode=dns.query.UDPMode.TRY_FIRST)
        expected = dns.zone.from_text(ixfr_expected, 'example')
        assert zone == expected

async def async_inbound_xfr():
    with XFRNanoNameserver() as ns:
        zone = dns.versioned.Zone('example')
        await dns.asyncquery.inbound_xfr(ns.tcp_address[0], zone,
                                         port=ns.tcp_address[1],
                                         udp_mode=dns.query.UDPMode.TRY_FIRST)
        await dns.asyncquery.inbound_xfr(ns.tcp_address[0], zone,
                                         port=ns.tcp_address[1],
                                         udp_mode=dns.query.UDPMode.TRY_FIRST)
        expected = dns.zone.from_text(ixfr_expected, 'example')
        assert zone == expected

@pytest.mark.skipif(not _nanonameserver_available,
                    reason="requires nanonameserver")
def test_asyncio_inbound_xfr():
    dns.asyncbackend.set_default_backend('asyncio')
    async def run():
        await async_inbound_xfr()
    try:
        runner = asyncio.run
    except AttributeError:
        # this is only needed for 3.6
        def old_runner(awaitable):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(awaitable)
        runner = old_runner
    runner(run())

#
# We don't need to do this as it's all generic code, but
# just for extra caution we do it for each backend.
#

try:
    import trio

    @pytest.mark.skipif(not _nanonameserver_available,
                        reason="requires nanonameserver")
    def test_trio_inbound_xfr():
        dns.asyncbackend.set_default_backend('trio')
        async def run():
            await async_inbound_xfr()
        trio.run(run)
except ImportError:
    pass

try:
    import curio

    @pytest.mark.skipif(not _nanonameserver_available,
                        reason="requires nanonameserver")
    def test_curio_inbound_xfr():
        dns.asyncbackend.set_default_backend('curio')
        async def run():
            await async_inbound_xfr()
        curio.run(run)
except ImportError:
    pass


class UDPXFRNanoNameserver(Server):

    def __init__(self):
        super().__init__(origin=dns.name.from_text('example'))
        self.did_truncation = False

    def handle(self, request):
        try:
            if request.message.question[0].rdtype == dns.rdatatype.IXFR:
                if self.did_truncation:
                    text = ixfr
                else:
                    text = retry_tcp_ixfr
                    self.did_truncation = True
            else:
                text = axfr
            r = dns.message.from_text(text, one_rr_per_rrset=True,
                                      origin=self.origin)
            r.id = request.message.id
            return r
        except Exception:
            pass

@pytest.mark.skipif(not _nanonameserver_available,
                    reason="requires nanonameserver")
def test_sync_retry_tcp_inbound_xfr():
    with UDPXFRNanoNameserver() as ns:
        zone = dns.versioned.Zone('example')
        dns.query.inbound_xfr(ns.tcp_address[0], zone, port=ns.tcp_address[1],
                              udp_mode=dns.query.UDPMode.TRY_FIRST)
        dns.query.inbound_xfr(ns.tcp_address[0], zone, port=ns.tcp_address[1],
                              udp_mode=dns.query.UDPMode.TRY_FIRST)
        expected = dns.zone.from_text(ixfr_expected, 'example')
        assert zone == expected

async def udp_async_inbound_xfr():
    with UDPXFRNanoNameserver() as ns:
        zone = dns.versioned.Zone('example')
        await dns.asyncquery.inbound_xfr(ns.tcp_address[0], zone,
                                         port=ns.tcp_address[1],
                                         udp_mode=dns.query.UDPMode.TRY_FIRST)
        await dns.asyncquery.inbound_xfr(ns.tcp_address[0], zone,
                                         port=ns.tcp_address[1],
                                         udp_mode=dns.query.UDPMode.TRY_FIRST)
        expected = dns.zone.from_text(ixfr_expected, 'example')
        assert zone == expected

@pytest.mark.skipif(not _nanonameserver_available,
                    reason="requires nanonameserver")
def test_asyncio_retry_tcp_inbound_xfr():
    dns.asyncbackend.set_default_backend('asyncio')
    async def run():
        await udp_async_inbound_xfr()
    try:
        runner = asyncio.run
    except AttributeError:
        def old_runner(awaitable):
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(awaitable)
        runner = old_runner
    runner(run())
