import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ydb.tools.ydb_bench.lib import monitoring_settings as monitoring
from ydb.tools.ydb_bench.lib.common import BenchmarkError


class Directory:
    def __init__(self, host_id):
        self.id = host_id
        self.peers = []

    def list(self):
        return [dict(id=p, name=p) for p in self.peers]

    def get(self, host_id):
        if host_id not in self.peers:
            raise BenchmarkError('unknown host')
        return dict(id=host_id)


class MonitoringSettingsTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.stores = {}
        self.offline = set()
        for host in ('a', 'b'):
            root = Path(self.temp.name) / host
            root.mkdir()
            self.stores[host] = monitoring.MonitoringSettings(root, Directory(host))
        for host, store in self.stores.items():
            store.hosts.peers = [h for h in self.stores if h != host]
        self.patch = mock.patch.object(monitoring, 'exchange', side_effect=self.exchange)
        self.patch.start()
        self.addCleanup(self.patch.stop)

    def exchange(self, host, operation, request=None):
        if host['id'] in self.offline:
            raise BenchmarkError('offline')
        store = self.stores[host['id']]
        return store.snapshot() if operation == 'snapshot' else store.save(request)

    def update(self, revision=0, **changes):
        return dict(
            revision=revision,
            settings=dict(
                dict(prometheus_url='http://prometheus:9090', prometheus_token='', grafana_url=''), **changes
            ),
        )

    def test_save_from_peer_and_restart(self):
        self.stores['b'].save(self.update())
        self.assertEqual(self.stores['a'].snapshot(), self.stores['b'].snapshot())
        self.assertEqual('a', self.stores['b'].snapshot()['owner_id'])
        restored = monitoring.MonitoringSettings(Path(self.temp.name) / 'a', Directory('a'))
        self.assertEqual(self.stores['a'].snapshot(), restored.snapshot())

    def test_stale_write_rejected(self):
        self.stores['a'].save(self.update())
        with self.assertRaises(BenchmarkError):
            self.stores['b'].save(self.update())
        self.assertEqual(1, self.stores['a'].snapshot()['revision'])

    def test_prometheus_generation_tracks_destination_and_token_not_grafana(self):
        store = self.stores['a']
        store.save(self.update())
        self.assertEqual(1, store.snapshot()['prometheus_revision'])
        store.save(self.update(1, grafana_url='http://grafana'))
        self.assertEqual(1, store.snapshot()['prometheus_revision'])
        store.save(self.update(2, prometheus_token='new-token'))
        self.assertEqual(3, store.snapshot()['prometheus_revision'])
        store.save(self.update(3))
        self.assertEqual(4, store.snapshot()['prometheus_revision'])
        self.stores['b'].sync()
        self.assertEqual(4, self.stores['b'].snapshot()['prometheus_revision'])

    def test_offline_peer_catches_up(self):
        self.stores['a'].save(self.update())
        self.offline.add('b')
        self.stores['a'].save(self.update(1))
        self.assertIsNone(self.stores['a'].status()['hosts'][0]['revision'])
        self.offline.clear()
        self.stores['b'].sync()
        self.assertEqual(2, self.stores['b'].snapshot()['revision'])

    def test_initialization_requires_reachable_peers(self):
        self.offline.add('b')
        with self.assertRaises(BenchmarkError):
            self.stores['a'].save(self.update())
        self.assertEqual(0, self.stores['a'].snapshot()['revision'])

    def test_owner_unavailable_does_not_fork(self):
        self.stores['b'].save(self.update())
        self.offline.add('a')
        with self.assertRaises(BenchmarkError):
            self.stores['b'].save(self.update(1))
        self.assertEqual(1, self.stores['b'].snapshot()['revision'])

    def test_new_host_adopts_existing_owner(self):
        self.stores['a'].save(self.update())
        root = Path(self.temp.name) / '0'
        root.mkdir()
        new = monitoring.MonitoringSettings(root, Directory('0'))
        new.hosts.peers = ['a', 'b']
        new.sync()
        self.assertEqual('a', new.snapshot()['owner_id'])

    def test_conflicting_owners_are_not_overwritten(self):
        for store in self.stores.values():
            store.hosts.peers = []
            store.save(self.update())
        self.stores['a'].hosts.peers = ['b']
        with self.assertRaises(BenchmarkError):
            self.stores['a'].sync()
        self.assertEqual('a', self.stores['a'].snapshot()['owner_id'])

    def test_url_validation(self):
        for url in (
            'javascript:alert(1)',
            'http://user:password@host',
            'http://host/?token=secret',
            'http://host:bad',
        ):
            with self.subTest(url=url), self.assertRaises(BenchmarkError):
                monitoring.validate_settings(dict(prometheus_url=url, prometheus_token='', grafana_url=''))
        self.assertEqual(
            '',
            monitoring.validate_settings(dict(prometheus_url='', prometheus_token='', grafana_url=''))[
                'prometheus_url'
            ],
        )

    def test_snapshot_is_detached(self):
        value = self.stores['a'].snapshot()
        value['settings']['prometheus_url'] = 'http://changed'
        self.assertEqual('', self.stores['a'].snapshot()['settings']['prometheus_url'])

    def test_token_is_redacted_preserved_and_removed(self):
        response = self.stores['b'].save(self.update(prometheus_token='test-token'))
        self.assertNotIn('prometheus_token', response['settings'])
        self.assertTrue(response['settings']['has_prometheus_token'])
        self.assertEqual('****oken', response['settings']['prometheus_token_mask'])
        self.stores['b'].save(self.update(1, prometheus_token=None))
        self.assertEqual('test-token', self.stores['a'].snapshot()['settings']['prometheus_token'])
        restored = monitoring.MonitoringSettings(Path(self.temp.name) / 'a', Directory('a'))
        self.assertEqual('test-token', restored.snapshot()['settings']['prometheus_token'])
        self.stores['b'].save(self.update(2, prometheus_token=''))
        self.assertFalse(self.stores['b'].status()['settings']['has_prometheus_token'])
        self.assertEqual('', self.stores['b'].status()['settings']['prometheus_token_mask'])

    def test_short_token_is_not_disclosed(self):
        response = self.stores['a'].save(self.update(prometheus_token='abcd'))
        self.assertEqual('****', response['settings']['prometheus_token_mask'])

    def test_grafana_token_is_redacted_and_preserved_by_old_clients(self):
        response = self.stores['a'].save(self.update(grafana_token='grafana-secret', grafana_api_url='http://internal'))
        self.assertNotIn('grafana_token', response['settings'])
        self.assertEqual('****cret', response['settings']['grafana_token_mask'])
        self.stores['a'].save(self.update(1))
        self.assertEqual('grafana-secret', self.stores['a'].snapshot()['settings']['grafana_token'])
        self.assertEqual('http://internal', self.stores['a'].snapshot()['settings']['grafana_api_url'])
        self.assertEqual(1, self.stores['a'].snapshot()['prometheus_revision'])
        self.stores['a'].save(self.update(2, grafana_token=''))
        self.assertFalse(self.stores['a'].status()['settings']['has_grafana_token'])

    def test_legacy_settings_migration(self):
        value = monitoring.MonitoringSettings._validate(
            dict(
                owner_id='a',
                revision=1,
                settings=dict(enabled=True, import_url='http://old-import', grafana_url='http://grafana'),
            )
        )
        self.assertEqual('', value['settings']['prometheus_url'])
        self.assertEqual('http://grafana', value['settings']['grafana_url'])

    def test_invalid_token_not_echoed(self):
        with self.assertRaisesRegex(BenchmarkError, '^invalid Prometheus token$'):
            self.stores['a'].save(self.update(prometheus_token='secret\r\nheader'))
