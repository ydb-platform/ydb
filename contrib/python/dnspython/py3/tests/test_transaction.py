# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

import time

import pytest

import dns.name
import dns.rdataclass
import dns.rdatatype
import dns.rdataset
import dns.rrset
import dns.transaction
import dns.versioned
import dns.zone


class DB(dns.transaction.TransactionManager):
    def __init__(self):
        self.rdatasets = {}

    def reader(self):
        return Transaction(self, False, True)

    def writer(self, replacement=False):
        return Transaction(self, replacement, False)

    def origin_information(self):
        return (dns.name.from_text('example'), True, dns.name.empty)

    def get_class(self):
        return dns.rdataclass.IN


class Transaction(dns.transaction.Transaction):
    def __init__(self, db, replacement, read_only):
        super().__init__(db, replacement, read_only)
        self.rdatasets = {}
        if not replacement:
            self.rdatasets.update(db.rdatasets)

    @property
    def db(self):
        return self.manager

    def _get_rdataset(self, name, rdtype, covers):
        return self.rdatasets.get((name, rdtype, covers))

    def _put_rdataset(self, name, rdataset):
        self.rdatasets[(name, rdataset.rdtype, rdataset.covers)] = rdataset

    def _delete_name(self, name):
        remove = []
        for key in self.rdatasets.keys():
            if key[0] == name:
                remove.append(key)
        if len(remove) > 0:
            for key in remove:
                del self.rdatasets[key]

    def _delete_rdataset(self, name, rdtype, covers):
        del self.rdatasets[(name, rdtype, covers)]

    def _name_exists(self, name):
        for key in self.rdatasets.keys():
            if key[0] == name:
                return True
        return False

    def _changed(self):
        if self.read_only:
            return False
        else:
            return len(self.rdatasets) > 0

    def _end_transaction(self, commit):
        if commit:
            self.db.rdatasets = self.rdatasets

    def _set_origin(self, origin):
        pass

@pytest.fixture
def db():
    db = DB()
    rrset = dns.rrset.from_text('content', 300, 'in', 'txt', 'content')
    db.rdatasets[(rrset.name, rrset.rdtype, 0)] = rrset
    return db

def test_basic(db):
    # successful txn
    with db.writer() as txn:
        rrset = dns.rrset.from_text('foo', 300, 'in', 'a',
                                    '10.0.0.1', '10.0.0.2')
        txn.add(rrset)
        assert txn.name_exists(rrset.name)
    assert db.rdatasets[(rrset.name, rrset.rdtype, 0)] == \
        rrset
    # rollback
    with pytest.raises(Exception):
        with db.writer() as txn:
            rrset2 = dns.rrset.from_text('foo', 300, 'in', 'a',
                                         '10.0.0.3', '10.0.0.4')
            txn.add(rrset2)
            raise Exception()
    assert db.rdatasets[(rrset.name, rrset.rdtype, 0)] == \
        rrset
    with db.writer() as txn:
        txn.delete(rrset.name)
    assert db.rdatasets.get((rrset.name, rrset.rdtype, 0)) \
        is None

def test_get(db):
    with db.writer() as txn:
        content = dns.name.from_text('content', None)
        rdataset = txn.get(content, dns.rdatatype.TXT)
        assert rdataset is not None
        assert rdataset[0].strings == (b'content',)
        assert isinstance(rdataset, dns.rdataset.ImmutableRdataset)

def test_add(db):
    with db.writer() as txn:
        rrset = dns.rrset.from_text('foo', 300, 'in', 'a',
                                    '10.0.0.1', '10.0.0.2')
        txn.add(rrset)
        rrset2 = dns.rrset.from_text('foo', 300, 'in', 'a',
                                     '10.0.0.3', '10.0.0.4')
        txn.add(rrset2)
    expected = dns.rrset.from_text('foo', 300, 'in', 'a',
                                   '10.0.0.1', '10.0.0.2',
                                   '10.0.0.3', '10.0.0.4')
    assert db.rdatasets[(rrset.name, rrset.rdtype, 0)] == \
        expected

def test_replacement(db):
    with db.writer() as txn:
        rrset = dns.rrset.from_text('foo', 300, 'in', 'a',
                                    '10.0.0.1', '10.0.0.2')
        txn.add(rrset)
        rrset2 = dns.rrset.from_text('foo', 300, 'in', 'a',
                                     '10.0.0.3', '10.0.0.4')
        txn.replace(rrset2)
    assert db.rdatasets[(rrset.name, rrset.rdtype, 0)] == \
        rrset2

def test_delete(db):
    with db.writer() as txn:
        txn.delete(dns.name.from_text('nonexistent', None))
        content = dns.name.from_text('content', None)
        content2 = dns.name.from_text('content2', None)
        txn.delete(content)
        assert not txn.name_exists(content)
        txn.delete(content2, dns.rdatatype.TXT)
        rrset = dns.rrset.from_text('content', 300, 'in', 'txt', 'new-content')
        txn.add(rrset)
        assert txn.name_exists(content)
        txn.delete(content, dns.rdatatype.TXT)
        assert not txn.name_exists(content)
        rrset = dns.rrset.from_text('content2', 300, 'in', 'txt', 'new-content')
        txn.delete(rrset)
    content_keys = [k for k in db.rdatasets if k[0] == content]
    assert len(content_keys) == 0

def test_delete_exact(db):
    with db.writer() as txn:
        rrset = dns.rrset.from_text('content', 300, 'in', 'txt', 'bad-content')
        with pytest.raises(dns.transaction.DeleteNotExact):
            txn.delete_exact(rrset)
        rrset = dns.rrset.from_text('content2', 300, 'in', 'txt', 'bad-content')
        with pytest.raises(dns.transaction.DeleteNotExact):
            txn.delete_exact(rrset)
        with pytest.raises(dns.transaction.DeleteNotExact):
            txn.delete_exact(rrset.name)
        with pytest.raises(dns.transaction.DeleteNotExact):
            txn.delete_exact(rrset.name, dns.rdatatype.TXT)
        rrset = dns.rrset.from_text('content', 300, 'in', 'txt', 'content')
        txn.delete_exact(rrset)
    assert db.rdatasets.get((rrset.name, rrset.rdtype, 0)) \
        is None

def test_parameter_forms(db):
    with db.writer() as txn:
        foo = dns.name.from_text('foo', None)
        rdataset = dns.rdataset.from_text('in', 'a', 300,
                                          '10.0.0.1', '10.0.0.2')
        rdata1 = dns.rdata.from_text('in', 'a', '10.0.0.3')
        rdata2 = dns.rdata.from_text('in', 'a', '10.0.0.4')
        txn.add(foo, rdataset)
        txn.add(foo, 100, rdata1)
        txn.add(foo, 30, rdata2)
    expected = dns.rrset.from_text('foo', 30, 'in', 'a',
                                   '10.0.0.1', '10.0.0.2',
                                   '10.0.0.3', '10.0.0.4')
    assert db.rdatasets[(foo, rdataset.rdtype, 0)] == \
        expected
    with db.writer() as txn:
        txn.delete(foo, rdataset)
        txn.delete(foo, rdata1)
        txn.delete(foo, rdata2)
    assert db.rdatasets.get((foo, rdataset.rdtype, 0)) \
        is None

def test_bad_parameters(db):
    with db.writer() as txn:
        with pytest.raises(TypeError):
            txn.add(1)
        with pytest.raises(TypeError):
            rrset = dns.rrset.from_text('bar', 300, 'in', 'txt', 'bar')
            txn.add(rrset, 1)
        with pytest.raises(ValueError):
            foo = dns.name.from_text('foo', None)
            rdata = dns.rdata.from_text('in', 'a', '10.0.0.3')
            txn.add(foo, 0x100000000, rdata)
        with pytest.raises(TypeError):
            txn.add(foo)
        with pytest.raises(TypeError):
            txn.add()
        with pytest.raises(TypeError):
            txn.add(foo, 300)
        with pytest.raises(TypeError):
            txn.add(foo, 300, 'hi')
        with pytest.raises(TypeError):
            txn.add(foo, 'hi')
        with pytest.raises(TypeError):
            txn.delete()
        with pytest.raises(TypeError):
            txn.delete(1)

def test_cannot_store_non_origin_soa(db):
    with pytest.raises(ValueError):
        with db.writer() as txn:
            rrset = dns.rrset.from_text('foo', 300, 'in', 'SOA',
                                        '. . 1 2 3 4 5')
            txn.add(rrset)

example_text = """$TTL 3600
$ORIGIN example.
@ soa foo bar 1 2 3 4 5
@ ns ns1
@ ns ns2
ns1 a 10.0.0.1
ns2 a 10.0.0.2
$TTL 300
$ORIGIN foo.example.
bar mx 0 blaz
"""

example_text_output = """@ 3600 IN SOA foo bar 1 2 3 4 5
@ 3600 IN NS ns1
@ 3600 IN NS ns2
@ 3600 IN NS ns3
ns1 3600 IN A 10.0.0.1
ns2 3600 IN A 10.0.0.2
ns3 3600 IN A 10.0.0.3
"""

@pytest.fixture(params=[dns.zone.Zone, dns.versioned.Zone])
def zone(request):
    return dns.zone.from_text(example_text, zone_factory=request.param)

def test_zone_basic(zone):
    with zone.writer() as txn:
        txn.delete(dns.name.from_text('bar.foo', None))
        rd = dns.rdata.from_text('in', 'ns', 'ns3')
        txn.add(dns.name.empty, 3600, rd)
        rd = dns.rdata.from_text('in', 'a', '10.0.0.3')
        txn.add(dns.name.from_text('ns3', None), 3600, rd)
    output = zone.to_text()
    assert output == example_text_output

def test_explicit_rollback_and_commit(zone):
    with zone.writer() as txn:
        assert not txn.changed()
        txn.delete(dns.name.from_text('bar.foo', None))
        txn.rollback()
    assert zone.get_node('bar.foo') is not None
    with zone.writer() as txn:
        assert not txn.changed()
        txn.delete(dns.name.from_text('bar.foo', None))
        txn.commit()
    assert zone.get_node('bar.foo') is None
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.delete(dns.name.from_text('bar.foo', None))
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.add('bar.foo', 300, dns.rdata.from_text('in', 'txt', 'hi'))
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.replace('bar.foo', 300, dns.rdata.from_text('in', 'txt', 'hi'))
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.reader() as txn:
            txn.rollback()
            txn.get('bar.foo', 'in', 'mx')
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.delete_exact('bar.foo')
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.name_exists('bar.foo')
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.update_serial()
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.changed()
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.rollback()
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            txn.commit()
    with pytest.raises(dns.transaction.AlreadyEnded):
        with zone.writer() as txn:
            txn.rollback()
            for rdataset in txn:
                pass

def test_zone_changed(zone):
    # Read-only is not changed!
    with zone.reader() as txn:
        assert not txn.changed()
    # delete an existing name
    with zone.writer() as txn:
        assert not txn.changed()
        txn.delete(dns.name.from_text('bar.foo', None))
        assert txn.changed()
    # delete a nonexistent name
    with zone.writer() as txn:
        assert not txn.changed()
        txn.delete(dns.name.from_text('unknown.bar.foo', None))
        assert not txn.changed()
    # delete a nonexistent rdataset from an extant node
    with zone.writer() as txn:
        assert not txn.changed()
        txn.delete(dns.name.from_text('bar.foo', None), 'txt')
        assert not txn.changed()
    # add an rdataset to an extant Node
    with zone.writer() as txn:
        assert not txn.changed()
        txn.add('bar.foo', 300, dns.rdata.from_text('in', 'txt', 'hi'))
        assert txn.changed()
    # add an rdataset to a nonexistent Node
    with zone.writer() as txn:
        assert not txn.changed()
        txn.add('foo.foo', 300, dns.rdata.from_text('in', 'txt', 'hi'))
        assert txn.changed()

def test_zone_base_layer(zone):
    with zone.writer() as txn:
        # Get a set from the zone layer
        rdataset = txn.get(dns.name.empty, dns.rdatatype.NS, dns.rdatatype.NONE)
        expected = dns.rdataset.from_text('in', 'ns', 300, 'ns1', 'ns2')
        assert rdataset == expected

def test_zone_transaction_layer(zone):
    with zone.writer() as txn:
        # Make a change
        rd = dns.rdata.from_text('in', 'ns', 'ns3')
        txn.add(dns.name.empty, 3600, rd)
        # Get a set from the transaction layer
        expected = dns.rdataset.from_text('in', 'ns', 300, 'ns1', 'ns2', 'ns3')
        rdataset = txn.get(dns.name.empty, dns.rdatatype.NS, dns.rdatatype.NONE)
        assert rdataset == expected
        assert txn.name_exists(dns.name.empty)
        ns1 = dns.name.from_text('ns1', None)
        assert txn.name_exists(ns1)
        ns99 = dns.name.from_text('ns99', None)
        assert not txn.name_exists(ns99)

def test_zone_add_and_delete(zone):
    with zone.writer() as txn:
        a99 = dns.name.from_text('a99', None)
        a100 = dns.name.from_text('a100', None)
        a101 = dns.name.from_text('a101', None)
        rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.99')
        txn.add(a99, rds)
        txn.delete(a99, dns.rdatatype.A)
        txn.delete(a100, dns.rdatatype.A)
        txn.delete(a101)
        assert not txn.name_exists(a99)
        assert not txn.name_exists(a100)
        assert not txn.name_exists(a101)
        ns1 = dns.name.from_text('ns1', None)
        txn.delete(ns1, dns.rdatatype.A)
        assert not txn.name_exists(ns1)
    with zone.writer() as txn:
        txn.add(a99, rds)
        txn.delete(a99)
        assert not txn.name_exists(a99)
    with zone.writer() as txn:
        txn.add(a100, rds)
        txn.delete(a99)
        assert not txn.name_exists(a99)
        assert txn.name_exists(a100)

def test_write_after_rollback(zone):
    with pytest.raises(ExpectedException):
        with zone.writer() as txn:
            a99 = dns.name.from_text('a99', None)
            rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.99')
            txn.add(a99, rds)
            raise ExpectedException
    with zone.writer() as txn:
        a99 = dns.name.from_text('a99', None)
        rds = dns.rdataset.from_text('in', 'a', 300, '10.99.99.99')
        txn.add(a99, rds)
    assert zone.get_rdataset('a99', 'a') == rds

def test_zone_get_deleted(zone):
    with zone.writer() as txn:
        ns1 = dns.name.from_text('ns1', None)
        assert txn.get(ns1, dns.rdatatype.A) is not None
        txn.delete(ns1)
        assert txn.get(ns1, dns.rdatatype.A) is None
        ns2 = dns.name.from_text('ns2', None)
        txn.delete(ns2, dns.rdatatype.A)
        assert txn.get(ns2, dns.rdatatype.A) is None

def test_zone_bad_class(zone):
    with zone.writer() as txn:
        rds = dns.rdataset.from_text('ch', 'ns', 300, 'ns1', 'ns2')
        with pytest.raises(ValueError):
            txn.add(dns.name.empty, rds)
        with pytest.raises(ValueError):
            txn.replace(dns.name.empty, rds)
        with pytest.raises(ValueError):
            txn.delete(dns.name.empty, rds)

def test_update_serial(zone):
    # basic
    with zone.writer() as txn:
        txn.update_serial()
    rdataset = zone.find_rdataset('@', 'soa')
    assert rdataset[0].serial == 2
    # max
    with zone.writer() as txn:
        txn.update_serial(0xffffffff, False)
    rdataset = zone.find_rdataset('@', 'soa')
    assert rdataset[0].serial == 0xffffffff
    # wraparound to 1
    with zone.writer() as txn:
        txn.update_serial()
    rdataset = zone.find_rdataset('@', 'soa')
    assert rdataset[0].serial == 1
    # trying to set to zero sets to 1
    with zone.writer() as txn:
        txn.update_serial(0, False)
    rdataset = zone.find_rdataset('@', 'soa')
    assert rdataset[0].serial == 1
    with pytest.raises(KeyError):
        with zone.writer() as txn:
            txn.update_serial(name=dns.name.from_text('unknown', None))
    with pytest.raises(ValueError):
        with zone.writer() as txn:
            txn.update_serial(-1)
    with pytest.raises(ValueError):
        with zone.writer() as txn:
            txn.update_serial(2**31)

class ExpectedException(Exception):
    pass

def test_zone_rollback(zone):
    a99 = dns.name.from_text('a99.example.')
    try:
        with zone.writer() as txn:
            rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.99')
            txn.add(a99, rds)
            assert txn.name_exists(a99)
            raise ExpectedException
    except ExpectedException:
        pass
    assert not zone.get_node(a99)

def test_zone_ooz_name(zone):
    with zone.writer() as txn:
        with pytest.raises(KeyError):
            a99 = dns.name.from_text('a99.not-example.')
            assert txn.name_exists(a99)

def test_zone_iteration(zone):
    expected = {}
    for (name, rdataset) in zone.iterate_rdatasets():
        expected[(name, rdataset.rdtype, rdataset.covers)] = rdataset
    with zone.writer() as txn:
        actual = {}
        for (name, rdataset) in txn:
            actual[(name, rdataset.rdtype, rdataset.covers)] = rdataset
    assert actual == expected

def test_iteration_in_replacement_txn(zone):
    rds = dns.rdataset.from_text('in', 'a', 300, '1.2.3.4', '5.6.7.8')
    expected = {}
    expected[(dns.name.empty, rds.rdtype, rds.covers)] = rds
    with zone.writer(True) as txn:
        txn.replace(dns.name.empty, rds)
        actual = {}
        for (name, rdataset) in txn:
            actual[(name, rdataset.rdtype, rdataset.covers)] = rdataset
    assert actual == expected

def test_replacement_commit(zone):
    rds = dns.rdataset.from_text('in', 'a', 300, '1.2.3.4', '5.6.7.8')
    expected = {}
    expected[(dns.name.empty, rds.rdtype, rds.covers)] = rds
    with zone.writer(True) as txn:
        txn.replace(dns.name.empty, rds)
    with zone.reader() as txn:
        actual = {}
        for (name, rdataset) in txn:
            actual[(name, rdataset.rdtype, rdataset.covers)] = rdataset
    assert actual == expected

def test_replacement_get(zone):
    with zone.writer(True) as txn:
        rds = txn.get(dns.name.empty, 'soa')
        assert rds is None


@pytest.fixture
def vzone():
    return dns.zone.from_text(example_text, zone_factory=dns.versioned.Zone)

def test_vzone_read_only(vzone):
    with vzone.reader() as txn:
        rdataset = txn.get(dns.name.empty, dns.rdatatype.NS, dns.rdatatype.NONE)
        expected = dns.rdataset.from_text('in', 'ns', 300, 'ns1', 'ns2')
        assert rdataset == expected
        with pytest.raises(dns.transaction.ReadOnly):
            txn.replace(dns.name.empty, expected)

def test_vzone_multiple_versions(vzone):
    assert len(vzone._versions) == 1
    vzone.set_max_versions(None)  # unlimited!
    with vzone.writer() as txn:
        txn.update_serial()
    with vzone.writer() as txn:
        txn.update_serial()
    with vzone.writer() as txn:
        txn.update_serial(1000, False)
    rdataset = vzone.find_rdataset('@', 'soa')
    assert rdataset[0].serial == 1000
    assert len(vzone._versions) == 4
    with vzone.reader(id=5) as txn:
        assert txn.version.id == 5
        rdataset = txn.get('@', 'soa')
        assert rdataset[0].serial == 1000
    with vzone.reader(serial=1000) as txn:
        assert txn.version.id == 5
        rdataset = txn.get('@', 'soa')
        assert rdataset[0].serial == 1000
    vzone.set_max_versions(2)
    assert len(vzone._versions) == 2
    # The ones that survived should be 3 and 1000
    rdataset = vzone._versions[0].get_rdataset(dns.name.empty,
                                               dns.rdatatype.SOA,
                                               dns.rdatatype.NONE)
    assert rdataset[0].serial == 3
    rdataset = vzone._versions[1].get_rdataset(dns.name.empty,
                                               dns.rdatatype.SOA,
                                               dns.rdatatype.NONE)
    assert rdataset[0].serial == 1000
    with pytest.raises(ValueError):
        vzone.set_max_versions(0)

# for debugging if needed
def _dump(zone):
    for v in zone._versions:
        print('VERSION', v.id)
        for (name, n) in v.nodes.items():
            for rdataset in n:
                print(rdataset.to_text(name))

def test_vzone_open_txn_pins_versions(vzone):
    assert len(vzone._versions) == 1
    vzone.set_max_versions(None)  # unlimited!
    with vzone.writer() as txn:
        txn.update_serial()
    with vzone.writer() as txn:
        txn.update_serial()
    with vzone.writer() as txn:
        txn.update_serial()
    with vzone.reader(id=2) as txn:
        vzone.set_max_versions(1)
        with vzone.reader(id=3) as txn:
            rdataset = txn.get('@', 'soa')
            assert rdataset[0].serial == 2
            assert len(vzone._versions) == 4
    assert len(vzone._versions) == 1
    rdataset = vzone.find_rdataset('@', 'soa')
    assert vzone._versions[0].id == 5
    assert rdataset[0].serial == 4


try:
    import threading

    one_got_lock = threading.Event()

    def run_one(zone):
        with zone.writer() as txn:
            one_got_lock.set()
            # wait until two blocks
            while len(zone._write_waiters) == 0:
                time.sleep(0.01)
            rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.98')
            txn.add('a98', rds)

    def run_two(zone):
        # wait until one has the lock so we know we will block if we
        # get the call done before the sleep in one completes
        one_got_lock.wait()
        with zone.writer() as txn:
            rds = dns.rdataset.from_text('in', 'a', 300, '10.0.0.99')
            txn.add('a99', rds)

    def test_vzone_concurrency(vzone):
        t1 = threading.Thread(target=run_one, args=(vzone,))
        t1.start()
        t2 = threading.Thread(target=run_two, args=(vzone,))
        t2.start()
        t1.join()
        t2.join()
        with vzone.reader() as txn:
            assert txn.name_exists('a98')
            assert txn.name_exists('a99')

except ImportError:  # pragma: no cover
    pass
