
import dns.rdata
import dns.rdataset


def test_processing_order_shuffle():
    rds = dns.rdataset.from_text('in', 'a', 300,
                                 '10.0.0.1', '10.0.0.2', '10.0.0.3')
    seen = set()
    for i in range(100):
        po = rds.processing_order()
        assert len(po) == 3
        for j in range(3):
            assert rds[j] in po
        seen.add(tuple(po))
    assert len(seen) == 6


def test_processing_order_priority_mx():
    rds = dns.rdataset.from_text('in', 'mx', 300,
                                 '10 a', '20 b', '20 c')
    seen = set()
    for i in range(100):
        po = rds.processing_order()
        assert len(po) == 3
        for j in range(3):
            assert rds[j] in po
        assert rds[0] == po[0]
        seen.add(tuple(po))
    assert len(seen) == 2


def test_processing_order_priority_weighted():
    rds = dns.rdataset.from_text('in', 'srv', 300,
                                 '1 10 1234 a', '2 90 1234 b', '2 10 1234 c')
    seen = set()
    weight_90_count = 0
    weight_10_count = 0
    for i in range(100):
        po = rds.processing_order()
        assert len(po) == 3
        for j in range(3):
            assert rds[j] in po
        assert rds[0] == po[0]
        if po[1].weight == 90:
            weight_90_count += 1
        else:
            assert po[1].weight == 10
            weight_10_count += 1
        seen.add(tuple(po))
    assert len(seen) == 2
    # We can't assert anything with certainty given these are random
    # draws, but it's super likely that weight_90_count > weight_10_count,
    # so we just assert that.
    assert weight_90_count > weight_10_count


def test_processing_order_priority_naptr():
    rds = dns.rdataset.from_text('in', 'naptr', 300,
                                 '1 10 a b c foo.', '1 20 a b c foo.',
                                 '2 10 a b c foo.', '2 10 d e f bar.')
    seen = set()
    for i in range(100):
        po = rds.processing_order()
        assert len(po) == 4
        for j in range(4):
            assert rds[j] in po
        assert rds[0] == po[0]
        assert rds[1] == po[1]
        seen.add(tuple(po))
    assert len(seen) == 2


def test_processing_order_empty():
    rds = dns.rdataset.from_text('in', 'naptr', 300)
    po = rds.processing_order()
    assert po == []


def test_processing_singleton_priority():
    rds = dns.rdataset.from_text('in', 'mx', 300, '10 a')
    po = rds.processing_order()
    assert po == [rds[0]]


def test_processing_singleton_weighted():
    rds = dns.rdataset.from_text('in', 'srv', 300, '1 10 1234 a')
    po = rds.processing_order()
    assert po == [rds[0]]


def test_processing_all_zero_weight_srv():
    rds = dns.rdataset.from_text('in', 'srv', 300,
                                 '1 0 1234 a', '1 0 1234 b', '1 0 1234 c')
    seen = set()
    for i in range(100):
        po = rds.processing_order()
        assert len(po) == 3
        for j in range(3):
            assert rds[j] in po
        seen.add(tuple(po))
    assert len(seen) == 6


def test_processing_order_uri():
    # We're testing here just to provide coverage for URI methods; the
    # testing of the weighting algorithm is done above in tests with
    # SRV.
    rds = dns.rdataset.from_text('in', 'uri', 300,
                                 '1 1 "ftp://ftp1.example.com/public"',
                                 '2 2 "ftp://ftp2.example.com/public"',
                                 '3 3 "ftp://ftp3.example.com/public"')
    po = rds.processing_order()
    assert len(po) == 3
    for i in range(3):
        assert po[i] == rds[i]


def test_processing_order_svcb():
    # We're testing here just to provide coverage for SVCB methods; the
    # testing of the priority algorithm is done above in tests with
    # MX and NAPTR.
    rds = dns.rdataset.from_text('in', 'svcb', 300,
                                 "1 . mandatory=alpn alpn=h2",
                                 "2 . mandatory=alpn alpn=h2",
                                 "3 . mandatory=alpn alpn=h2")
    po = rds.processing_order()
    assert len(po) == 3
    for i in range(3):
        assert po[i] == rds[i]
