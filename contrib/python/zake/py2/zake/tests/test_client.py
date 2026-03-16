# -*- coding: utf-8 -*-

# vim: tabstop=4 shiftwidth=4 softtabstop=4

#    Copyright (C) 2014 Yahoo! Inc. All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import collections
import contextlib
import sys
import threading
import unittest

import six

from kazoo import exceptions as k_exceptions
from kazoo.recipe import watchers as k_watchers

from zake import fake_client

# A reasonably high number to avoid waiting forever...
WAIT_TIME = 60


if sys.version_info[0:2] == (2, 6):
    class Event(threading._Event):
        def wait(self, timeout=None):
            self.__cond.acquire()
            try:
                if not self.__flag:
                    self.__cond.wait(timeout)
                return self.__flag
            finally:
                self.__cond.release()
else:
    Event = threading.Event


def make_daemon_thread(*args, **kwargs):
    t = threading.Thread(*args, **kwargs)
    t.daemon = True
    return t


@contextlib.contextmanager
def start_close(client):
    client.start()
    try:
        yield client
    finally:
        client.close()


class TestClient(unittest.TestCase):
    def setUp(self):
        super(TestClient, self).setUp()
        self.client = fake_client.FakeClient()
        self.addCleanup(self.client.close)

    def test_connected(self):
        self.assertFalse(self.client.connected)
        with start_close(self.client) as c:
            self.assertTrue(c.connected)

    def test_command(self):
        with start_close(self.client) as c:
            self.assertTrue(c.connected)
            self.assertEqual("imok", c.command(b'ruok'))
            self.client.command(b'kill')
            self.assertFalse(c.connected)

    def test_command_version(self):
        with start_close(self.client) as c:
            stats = c.command(b'stat')
            self.assertIn("standalone", stats)
            version = ".".join([str(s) for s in fake_client.SERVER_VERSION])
            self.assertIn(version, stats)

    def test_command_envi(self):
        with start_close(self.client) as c:
            envi = c.command(b'envi')
            self.assertIn("zookeeper.version", envi)
            version = ".".join([str(s) for s in fake_client.SERVER_VERSION])
            self.assertIn(version, envi)

    def test_command_empty_version(self):
        self.assertRaises(ValueError, fake_client.FakeClient,
                          server_version=[])

    def test_command_custom_version(self):
        client = fake_client.FakeClient(server_version=(1, 1, 1))
        with start_close(client) as c:
            stats = c.command(b'stat')
            self.assertIn("standalone", stats)
            self.assertIn('1.1.1', stats)

    def test_root(self):
        with start_close(self.client) as c:
            self.assertTrue(c.exists("/"))

    def test_version(self):
        with start_close(self.client) as c:
            self.assertTrue(len(c.server_version()) > 0)
            self.assertEqual(fake_client.SERVER_VERSION,
                             c.server_version())

    def test_make_path(self):
        with start_close(self.client) as c:
            c.create("/a/b/c", makepath=True)
            self.assertTrue(c.exists("/a/b/c"))
            self.assertTrue(c.exists("/a/b"))
            self.assertTrue(c.exists("/a"))

    def test_path_normalization(self):
        with start_close(self.client) as c:
            c.create("/a", b"blah", makepath=True)
            self.assertEqual(c.get("/a")[0], b"blah")
            self.assertEqual(c.get("a")[0], b"blah")

    def test_missing_leading_slash(self):
        with start_close(self.client) as c:
            c.create("a/b/c", b"blah", makepath=True)
            self.assertEqual(c.get("a/b/c")[0], b"blah")
            self.assertTrue(c.exists("a/b/c"))
            self.assertTrue(c.exists("a/b"))
            self.assertTrue(c.exists("a"))

    def test_no_make_path(self):
        with start_close(self.client) as c:
            self.assertRaises(k_exceptions.KazooException,
                              c.create, "/a/b/c")

    def test_concurrent_restart(self):
        accumulator = collections.deque()

        def do_restart():
            accumulator.append(self.client.restart())

        threads = []
        for i in range(0, 20):
            threads.append(make_daemon_thread(target=do_restart))
            threads[-1].start()
        while threads:
            t = threads.pop()
            t.join()

        self.assertEqual(20, len(accumulator))
        start_counts = collections.defaultdict(int)
        for before in accumulator:
            start_counts[before] += 1
        for before, count in six.iteritems(start_counts):
            self.assertEqual(1, count)

    def test_sequence(self):
        with start_close(self.client) as c:
            path = c.create("/", sequence=True)
            self.assertEqual("/0000000000", path)
            path = c.create("/", sequence=True)
            self.assertEqual("/0000000001", path)
            children = c.get_children("/")
            self.assertEqual(2, len(children))
            seqs = c.storage.sequences
            self.assertEqual(1, len(seqs))
            self.assertEqual(2, seqs['/'])

    def test_command_no_connect(self):
        self.assertRaises(k_exceptions.KazooException, self.client.sync, '/')

    def test_create(self):
        with start_close(self.client) as c:
            c.ensure_path("/b")
            c.create('/b/c', b'abc')
            paths = c.storage.paths
            self.assertEqual(3, len(paths))
            self.assertEqual(1, len(c.storage.get_children("/b")))
            self.assertEqual(1, len(c.storage.get_parents("/b")))
            data, znode = c.get("/b/c")
            self.assertEqual(b'abc', data)
            self.assertEqual(0, znode.version)
            c.set("/b/c", b"efg")
            data, znode = c.get("/b/c")
            self.assertEqual(b"efg", data)
            self.assertEqual(1, znode.version)

    def test_create_slashed(self):
        with start_close(self.client) as c:
            self.assertEqual("/b", c.create("/b"))
            self.assertEqual("/b0000000000", c.create("/b", sequence=True))
            c.create("/c0000000000")
            self.assertTrue(c.create("/c", sequence=True))
            self.assertRaises(k_exceptions.NoNodeError,
                              c.create, "/e/", sequence=True)
            self.assertRaises(k_exceptions.NodeExistsError,
                              c.create, "/b/")
            self.assertTrue(c.create("/b/", sequence=True))

    def test_ephemeral_raises(self):
        with start_close(self.client) as c:
            c.create("/b", ephemeral=True)
            data, znode = c.get("/b")
            self.assertNotEqual(0, znode.ephemeralOwner)
        with start_close(self.client) as c:
            self.assertRaises(k_exceptions.NoNodeError,
                              c.get, "/b")

    def test_ephemeral_no_children(self):
        with start_close(self.client) as c:
            c.create("/b", ephemeral=True)
            self.assertRaises(k_exceptions.NoChildrenForEphemeralsError,
                              c.create, "/b/c")
        with start_close(self.client) as c:
            c.create("/b", ephemeral=False)
            c.create("/b/c")

    def test_root_delete(self):
        with start_close(self.client) as c:
            self.assertRaises(k_exceptions.BadArgumentsError,
                              c.delete, '/')

    def test_delete(self):
        with start_close(self.client) as c:
            self.assertRaises(k_exceptions.NoNodeError, c.delete, "/b")
            c.ensure_path("/")
            c.create("/b", b'b')
            c.delete("/b")
            self.assertRaises(k_exceptions.NoNodeError, c.delete, "/b")
            self.assertFalse(c.exists("/b"))

    def test_get_children(self):
        with start_close(self.client) as c:
            c.ensure_path("/a/b")
            c.ensure_path("/a/c")
            c.ensure_path("/a/d")
            self.assertEqual(3, len(c.get_children("/a")))

    def test_exists(self):
        with start_close(self.client) as c:
            c.ensure_path("/a")
            self.assertTrue(c.exists("/"))
            self.assertTrue(c.exists("/a"))

    def test_sync(self):
        with start_close(self.client) as c:
            c.sync("/")

    def test_transaction(self):
        with start_close(self.client) as c:
            c.ensure_path("/b")
            with c.transaction() as txn:
                txn.create("/b/c")
                txn.set_data("/b/c", b'e')
            data, znode = c.get("/b/c")
            self.assertEqual(b'e', data)
            self.assertTrue(txn.committed)

    def test_transaction_check(self):
        with start_close(self.client) as c:
            c.create("/b")
            data, stat = c.get("/b")
            with c.transaction() as txn:
                txn.check("/e", 1)
                txn.create("/c")
            self.assertFalse(txn.committed)
            self.assertFalse(c.exists("/c"))
            with c.transaction() as txn:
                txn.check("/b", stat.version)
                txn.create("/c")
            self.assertTrue(txn.committed)
            self.assertTrue(c.exists("/c"))

    def test_transaction_abort(self):
        with start_close(self.client) as c:
            c.create("/b")
            data, stat = c.get("/b")
            txn = c.transaction()
            txn.create("/c")
            txn.check("/b", stat.version + 1)
            results = txn.commit()
            self.assertFalse(c.exists("/c"))
            self.assertEqual(2, len(results))
            self.assertIsInstance(results[1], k_exceptions.BadVersionError)
            self.assertFalse(txn.committed)

    def test_session_id(self):
        self.assertIsNone(self.client.session_id)
        with start_close(self.client) as c:
            self.assertIsNotNone(c.session_id)
        self.assertIsNone(self.client.session_id)

    def test_data_watch_not_triggered(self):
        ev = Event()
        updates = []

        def notify_me(data, stat):
            if stat:
                updates.append((data, stat))
                ev.set()

        k_watchers.DataWatch(self.client, "/b", func=notify_me)
        with start_close(self.client) as c:
            with c.transaction() as txn:
                txn.create("/b")
                txn.check("/c", version=0)
            self.assertEqual(0, len(updates))
            self.assertFalse(txn.committed)
            with c.transaction() as txn:
                txn.create("/b")
                txn.check("/b", version=0)
            self.assertTrue(txn.committed)
            self.assertTrue(ev.wait(WAIT_TIME))
            self.assertEqual(1, len(updates))

    def test_concurrent_transaction_aborts(self):

        def thread_create(client, path):
            with client.transaction() as txn:
                txn.create(path)
                txn.check(path, version=1)

        with start_close(self.client) as c:
            threads = []
            paths = []
            for i in range(0, 20):
                paths.append("/tmp%010d" % (i))
                t = make_daemon_thread(target=thread_create,
                                       args=(c, paths[-1]))
                threads.append(t)
                t.start()
            while threads:
                t = threads.pop()
                t.join()
            for p in paths:
                self.assertFalse(c.exists(p))

    def test_concurrent_transaction_half_work(self):
        results = collections.deque()

        def thread_create(client, path, data):
            txn = client.transaction()
            txn.create(path)
            txn.set_data(path, six.text_type(data).encode("utf8"))
            try:
                txn.commit()
            finally:
                results.append(txn.committed)

        with start_close(self.client) as c:
            threads = []
            paths = []
            for i in range(0, 20):
                paths.append("/tmp%010d" % (i % 10))
                t = make_daemon_thread(target=thread_create,
                                       args=(c, paths[-1], i))
                threads.append(t)
                t.start()
            while threads:
                t = threads.pop()
                t.join()
            names = set()
            for p in paths:
                self.assertTrue(c.exists(p))
                names.add(c.get(p)[0])
            passes = [r for r in results if r]
            failures = [r for r in results if not r]
            self.assertEqual(10, len(passes))
            self.assertEqual(10, len(failures))
            self.assertEqual(10, len(names))

    def test_data_watch(self):
        updates = collections.deque()
        ev = Event()

        def notify_me(data, stat):
            updates.append((data, stat))
            ev.set()

        k_watchers.DataWatch(self.client, "/b", func=notify_me)
        with start_close(self.client) as c:
            self.assertTrue(ev.wait(WAIT_TIME))
            ev.clear()
            c.ensure_path("/b")
            self.assertTrue(ev.wait(WAIT_TIME))
            ev.clear()
            c.set("/b", b"1")
            self.assertTrue(ev.wait(WAIT_TIME))
            ev.clear()
            c.set("/b", b"2")
            self.assertTrue(ev.wait(WAIT_TIME))
            ev.clear()

        self.assertEqual(4, len(updates))

        ev.clear()
        with start_close(self.client) as c:
            c.delete("/b")
            self.assertTrue(ev.wait(WAIT_TIME))

        self.assertEqual(5, len(updates))

    def test_recursive_delete(self):
        with start_close(self.client) as c:
            c.ensure_path("/b/c/d/e")
            c.ensure_path("/b/e/f/g")
            c.ensure_path("/b/123/abc")
            c.ensure_path("/a")
            c.delete("/b", recursive=True)
            self.assertTrue(c.get("/a"))
            self.assertEqual(2, len(c.storage.paths))

    def test_child_left_delete(self):
        with start_close(self.client) as c:
            c.ensure_path("/b/c/d/e")
            self.assertRaises(k_exceptions.NotEmptyError, c.delete,
                              "/b", recursive=False)

    def test_child_watch(self):
        updates = collections.deque()
        ev = Event()

        def one_time_collector_func(children):
            updates.extend(children)
            if children:
                ev.set()
                return False

        with start_close(self.client) as c:
            k_watchers.ChildrenWatch(self.client, "/",
                                     func=one_time_collector_func)
            c.ensure_path("/b")
            self.assertTrue(ev.wait(WAIT_TIME))

        self.assertEqual(['b'], list(updates))

    def test_child_child_watch(self):
        updates = collections.deque()
        ev = Event()

        def one_time_collector_func(children):
            updates.extend(children)
            if children:
                ev.set()
                return False

        with start_close(self.client) as c:
            c.ensure_path("/b")
            k_watchers.ChildrenWatch(self.client, "/b",
                                     func=one_time_collector_func)
            c.ensure_path("/b/c")
            self.assertTrue(ev.wait(WAIT_TIME))

        self.assertEqual(['c'], list(updates))

    # TODO: this fails for some reason
    # def test_child_watch_no_create(self):
    #    cb = lambda *args, **kwargs: None
    #    with start_close(self.client) as c:
    #        self.assertRaises(k_exceptions.NoNodeError,
    #                          k_watchers.ChildrenWatch,
    #                          self.client, "/b", cb)

    def test_create_sequence(self):
        with start_close(self.client) as c:
            path = c.create("/a", sequence=True)
            self.assertEqual('/a0000000000', path)
            c.ensure_path("/b/")
            path = c.create("/b", sequence=True)
            self.assertEqual('/b0000000001', path)
            path = c.create("/b/", sequence=True)
            self.assertEqual('/b/0000000000', path)

    def test_create_async(self):
        with start_close(self.client) as c:
            r = c.create_async("/b")
            self.assertEqual("/b", r.get())
            self.assertTrue(r.successful())
            self.assertIsNone(r.exception)

    def test_create_async_linked(self):
        traces = collections.deque()
        ev = Event()

        def add_trace(result):
            traces.append(result)
            ev.set()

        with start_close(self.client) as c:
            r = c.create_async("/b")
            r.rawlink(add_trace)
            self.assertEqual("/b", r.get())
            self.assertTrue(ev.wait(WAIT_TIME))

        self.assertEqual(1, len(traces))
        self.assertEqual(r, traces[0])

    def test_create_async_exception(self):
        ev = Event()

        def wait_for(result):
            ev.set()

        with start_close(self.client) as c:
            r = c.create_async("/b/c/d")
            r.rawlink(wait_for)
            self.assertTrue(ev.wait(WAIT_TIME))
            self.assertFalse(r.successful())
            self.assertIsNotNone(r.exception)


class TestMultiClient(unittest.TestCase):
    def make_clients(self, count, shared_storage=True):
        clients = []
        storage = None
        for _i in range(0, count):
            if storage is None:
                client = fake_client.FakeClient()
                self.addCleanup(client.close)
                storage = client.storage
            else:
                client = fake_client.FakeClient(storage=storage)
                self.addCleanup(client.close)
            clients.append(client)
        return clients

    def test_clients_counter(self):
        clients = self.make_clients(25)

        def increment(client):
            with start_close(client):
                counter = client.Counter("/int")
                counter += 2

        threads = []
        for i in range(0, len(clients)):
            threads.append(make_daemon_thread(target=increment,
                                              args=(clients[i],)))
        try:
            for t in threads:
                t.start()
        finally:
            while threads:
                t = threads.pop()
                t.join()

        data, znode = clients[0].storage.get("/int")
        self.assertEqual(2 * len(clients), int(data))

    def test_clients_attached(self):
        clients = self.make_clients(50)
        for i, c in enumerate(clients):
            c.start()
            self.assertEqual(i + 1, len(c.storage.clients))
        total = len(c.storage.clients)
        self.assertEqual(50, total)
        for i, c in enumerate(clients):
            c.close()
            total -= 1
            self.assertEqual(total, len(c.storage.clients))
        self.assertEqual(0, total)

    def test_clients_triggered(self):
        client1, client2 = self.make_clients(2)
        ev = Event()

        @client1.DataWatch("/b")
        def cb(data, stat):
            if data == b'fff':
                ev.set()

        with start_close(client1):
            client1.create('/a')
            client1.set('/a', b'b')
            with start_close(client2):
                value, znode = client2.get('/a')
                self.assertEqual(b'b', value)
                client2.create("/b", b'eee')
                client2.set("/b", b'fff')
            self.assertTrue(ev.wait(WAIT_TIME))

    def test_purge_clients_triggered(self):
        client1, client2 = self.make_clients(2)
        events = collections.deque()
        fff_rcv = Event()
        end_rcv = Event()

        @client1.DataWatch("/b")
        def cb(data, stat):
            events.append((data, stat))
            if data == b'fff':
                fff_rcv.set()
            if data is None and fff_rcv.is_set():
                end_rcv.set()

        with start_close(client1):
            client1.create('/a')
            client1.set('/a', b'b')
            with start_close(client2):
                value, znode = client2.get('/a')
                self.assertEqual(b'b', value)
                client2.create("/b", b'eee', ephemeral=True)
                client2.set("/b", b'fff')
                self.assertTrue(fff_rcv.wait(WAIT_TIME))
            self.assertTrue(end_rcv.wait(WAIT_TIME))

        self.assertEqual((None, None), events[-1])
