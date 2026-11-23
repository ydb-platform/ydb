import tempfile
import shutil
import subprocess
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from ydb.tools.ydb_bench.lib import run_index, web
from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json
from ydb.tools.ydb_bench.lib.federation import Federation, page_options


class RunIndexTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write(self, name, profile='main'):
        path = self.root / name / 'run.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_json(
            path,
            {
                'schema_version': 4,
                'status': 'completed',
                'state': 'completed',
                'started_at': '2026-09-15T10:00:00+00:00',
                'finished_at': '2026-09-15T10:00:10+00:00',
                'steps': [{'benchmark': 'local-ydb', 'profile': profile, 'state': 'passed'}],
            },
        )
        return path

    def index(self):
        index = run_index.RunIndex(self.root, web.run_record, interval=3600)
        self.addCleanup(index.close)
        return index

    def test_incremental_reconcile_and_restart(self):
        path = self.write('one')
        index = self.index()
        with mock.patch.object(run_index, 'load_manifest', wraps=run_index.load_manifest) as read:
            self.assertEqual(len(index.query({})), 1)
            index.reconcile()
            self.assertEqual(read.call_count, 0)
            self.write('one', profile='changed')
            index.refresh([path])
            self.assertEqual(read.call_count, 1)
            self.assertEqual(index.query({'profile': 'main'}), [])
            self.assertEqual(index.query({'profile': 'changed'})[0]['id'], 'one')
            self.assertNotIn('steps', index.query({})[0])
            self.write('imported/nested')
            index.reconcile()
            self.assertEqual(len(index.query({})), 2)
            path.unlink()
            index.reconcile()
            self.assertEqual([r['id'] for r in index.query({})], ['imported/nested'])
        index.close()
        with mock.patch.object(run_index, 'load_manifest', wraps=run_index.load_manifest) as read:
            restored = self.index()
            self.assertEqual(len(restored.query({})), 1)
            self.assertEqual(read.call_count, 0)

    def test_corrupt_index_is_preserved_and_rebuilt(self):
        self.write('one')
        (self.root / '.run-index.sqlite3').write_bytes(b'not sqlite')
        index = self.index()
        self.assertEqual(len(index.query({})), 1)
        self.assertEqual(len(list(self.root.glob('.run-index.sqlite3.invalid-*'))), 1)

    def test_writer_notifications_are_incremental(self):
        index = self.index()
        path = self.write('new')
        store = web.ResultStore(path, run_index.load_manifest(path), on_write=index.mark_dirty)
        store.write()
        with mock.patch.object(run_index, 'load_manifest', wraps=run_index.load_manifest) as read:
            index.refresh_pending()
            self.assertEqual(read.call_count, 1)
            index.refresh_pending()
            self.assertEqual(read.call_count, 1)
        self.assertEqual(index.query({})[0]['id'], 'new')

    @unittest.skipUnless(shutil.which('node'), 'node is required for pager checks')
    def test_browser_pager_ignores_stale_responses(self):
        script = web._JS[web._JS.index('function bindRunPager(') : web._JS.index('async function renderRuns()')]
        script += r"""
const assert=require('assert');
const elements=new Map(),container={querySelector:key=>{if(!elements.has(key))elements.set(key,{});return elements.get(key)}};
let requests=[],drawn=[],filter='';
const api=(url,options)=>new Promise(resolve=>requests.push({url,options,resolve}));
const federationErrors=()=>'',displayError=e=>e.message;
const pager=bindRunPager(container,()=>new URLSearchParams({query:filter}),value=>drawn.push(value.entries),()=>true);
const response=(id,next=null)=>({entries:[id],errors:[],next_cursor:next});
(async()=>{
  const first=pager.load();requests[0].resolve(response('first','cursor-1'));await first;
  elements.get('[data-page-next]').onclick();
  assert(requests[1].url.includes('cursor=cursor-1'));
  filter='new';const filtered=pager.load();
  assert(requests[1].options.signal.aborted);
  requests[2].resolve(response('filtered'));await filtered;
  requests[1].resolve(response('stale'));await new Promise(resolve=>setImmediate(resolve));
  assert.deepStrictEqual(drawn,[['first'],['filtered']]);
  assert.equal(elements.get('[data-page-prev]').disabled,true);
  assert.equal(elements.get('[data-page-next]').disabled,true);
  assert(!requests[2].url.includes('cursor='));
})().catch(error=>{console.error(error);process.exitCode=1});
"""
        subprocess.run([shutil.which('node'), '-e', script], check=True, timeout=10)

    def test_filters_and_cursor(self):
        for name in ('a', 'b', 'c'):
            self.write(name, profile='exact_%')
        index = self.index()
        self.assertEqual(len(index.query({'profile': 'exact_%'})), 3)
        self.assertEqual(index.query({'profile': '%'}), [])
        self.assertEqual(len(index.query({'until': '2026-09-15', 'since': '2026-09-15'})), 3)
        self.assertEqual(index.query({'since': '2026-09-16'}), [])
        self.assertEqual([r['id'] for r in index.query({'selected': ['host:b']}, host_id='host')], ['b'])
        first = index.query({}, limit=2)
        after = [run_index.order_value(first[-1], 'newest'), 'host', first[-1]['id']]
        self.assertEqual([r['id'] for r in index.query({}, limit=2, after=after, host_id='host')], ['c'])
        with self.assertRaises(BenchmarkError):
            page_options({'limit': ['100000']})
        with self.assertRaises(BenchmarkError):
            page_options({'cursor': ['bad']})

    def test_federated_pages_with_ties_and_partial_failure(self):
        for name in ('a', 'b', 'c'):
            self.write(name)
        index = self.index()
        directory = SimpleNamespace(
            port=1, identity=lambda port: {'id': 'host-a', 'name': 'A'}, list=lambda: [{'id': 'host-b', 'name': 'B'}]
        )
        federation = Federation(SimpleNamespace(hosts=directory))

        def read(host, path, local):
            from urllib.parse import parse_qs, urlparse

            filters, order, limit, after, _ = page_options(parse_qs(urlparse(path).query))
            return {'entries': index.query(filters, order, limit + 1, after, host), 'benchmarks': ['local-ydb']}

        with mock.patch.object(federation, 'read', side_effect=read):
            for order in ('oldest', 'newest', 'longest'):
                query = {'limit': ['2'], 'sort': [order]}
                ids = []
                for _ in range(4):
                    page = federation.run_page(query)
                    ids.extend(row['id'] for row in page['entries'])
                    if not page['next_cursor']:
                        break
                    query['cursor'] = [page['next_cursor']]
                self.assertEqual(ids, ['host-a:a', 'host-a:b', 'host-a:c', 'host-b:a', 'host-b:b', 'host-b:c'])
                query['profile'] = ['different']
                with self.assertRaises(BenchmarkError):
                    federation.run_page(query)
        with mock.patch.object(federation, 'read', side_effect=BenchmarkError('offline')):
            page = federation.run_page({})
            self.assertEqual(len(page['errors']), 2)
            self.assertIsNone(page['next_cursor'])
