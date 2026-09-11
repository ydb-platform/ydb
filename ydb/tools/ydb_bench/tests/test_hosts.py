import json
from contextlib import ExitStack
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid
import threading
import shutil
import subprocess
from urllib.request import Request, urlopen
from urllib.error import HTTPError

from ydb.tools.ydb_bench.lib import hosts
from ydb.tools.ydb_bench.lib import web
from ydb.tools.ydb_bench.lib.federation import Federation, reference, split_reference
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.web import make_server


class HostsTest(unittest.TestCase):
    @unittest.skipUnless(shutil.which('node'), 'node is required for editor routing checks')
    def test_editor_routes_to_selected_host(self):
        helpers = web._JS.split("let editorHost=", 1)[1].split('function runDisplay', 1)[0]
        script = "let editorHost=" + helpers + """
const assert=require('assert'),enc=encodeURIComponent;
const api=(path,options)=>({path,options});
assert.deepStrictEqual(editorApi('/api/editor-config',{body:'yaml'}),{path:'/api/editor-config',options:{body:'yaml'}});
editorHost='peer-id';
for(const path of ['/api/editor-config','/api/validate','/api/drafts','/api/runs']){
  assert.strictEqual(editorApi(path,{}).path,'/api/hosts/peer-id'+path);
}
"""
        subprocess.run([shutil.which('node'), '-e', script], check=True, capture_output=True, timeout=10)

    @unittest.skipUnless(shutil.which('node'), 'node is required for remote report checks')
    def test_remote_run_chart_route(self):
        split = 'function splitRunRef' + web._JS.split('function splitRunRef', 1)[1].split('let editorHost', 1)[0]
        loader = (
            'async function loadChartData'
            + web._JS.split('async function loadChartData', 1)[1].split('async function loadLocalYdbComparison', 1)[0]
        )
        script = split + loader + """
const assert=require('assert'),enc=encodeURIComponent,api=async path=>path;
(async()=>{
  const host='60834016-4866-405f-bdbc-63271c093b06';
  assert.strictEqual(await loadChartData([host+':same-run'],'ping-bench'),
    '/api/hosts/'+host+'/api/chart-data?run=same-run&benchmark=ping-bench');
  assert.strictEqual(await loadChartData(['local-run']),'/api/chart-data?run=local-run');
})().catch(error=>{console.error(error);process.exitCode=1});
"""
        subprocess.run([shutil.which('node'), '-e', script], check=True, capture_output=True, timeout=10)

    def test_remote_editor_and_launch(self):
        with ExitStack() as stack:
            servers = [
                make_server('127.0.0.1', 0, stack.enter_context(tempfile.TemporaryDirectory())) for _ in range(2)
            ]
            threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
            for thread in threads:
                thread.start()
            try:
                local, peer = servers
                directory = local.service.hosts
                directory.add({'endpoint': peer.service.hosts.endpoint, 'token': peer.service.hosts.token})
                base = directory.endpoint + '/api/hosts/' + peer.service.hosts.id
                payload = {'yaml': 'test configuration', 'perf': True, 'continue_on_error': True}

                def post(url, options=payload, headers=None):
                    return urlopen(
                        Request(
                            url,
                            data=json.dumps(options).encode(),
                            headers={'Content-Type': 'application/json', **(headers or {})},
                        ),
                        timeout=5,
                    )

                for path, method, result, args in (
                    (
                        'editor-config',
                        'editor_config',
                        {'binary_catalog': {'ydbd': ['peer-binary']}},
                        ('test configuration', True),
                    ),
                    (
                        'validate',
                        'validate',
                        {'valid': False, 'error': 'remote validation'},
                        ('test configuration', True),
                    ),
                    ('plan', 'plan', {'valid': True}, ('test configuration', True)),
                    ('drafts', 'save_draft', {'path': 'peer-draft'}, ('test configuration',)),
                    ('runs', 'start', {'id': 'peer-run'}, ('test configuration', True, True)),
                ):
                    with mock.patch.object(peer.service, method, return_value=result) as remote_call, mock.patch.object(
                        local.service, method, side_effect=AssertionError('must not execute locally')
                    ):
                        with post(base + '/api/' + path) as response:
                            self.assertEqual(json.load(response), result)
                        remote_call.assert_called_once_with(*args)
                with mock.patch.object(peer.service, 'start', side_effect=BenchmarkError('invalid peer binary')):
                    with self.assertRaises(HTTPError) as error:
                        post(base + '/api/runs')
                    self.assertEqual(error.exception.code, 400)
                    self.assertEqual(json.load(error.exception)['error'], 'invalid peer binary')
                with mock.patch.object(peer.service, 'cancel', return_value={'id': 'peer-run'}) as cancel:
                    with post(base + '/api/runs/peer-run/cancel', {}) as response:
                        self.assertEqual(json.load(response), {'id': 'peer-run'})
                    cancel.assert_called_once_with('peer-run')
                for url, headers, status in (
                    (base + '/api/runs', {'Origin': 'http://other.example'}, 403),
                    (peer.service.hosts.endpoint + '/peer/api/runs', {}, 401),
                    (
                        peer.service.hosts.endpoint + '/peer/api/runs',
                        {'Authorization': 'Bearer ' + peer.service.hosts.token, 'Origin': directory.endpoint},
                        403,
                    ),
                    (
                        peer.service.hosts.endpoint + '/peer/api/import',
                        {'Authorization': 'Bearer ' + peer.service.hosts.token},
                        403,
                    ),
                ):
                    with self.assertRaises(HTTPError) as error:
                        post(url, headers=headers)
                    self.assertEqual(error.exception.code, status)
            finally:
                for server in servers:
                    server.shutdown()
                    server.server_close()
                for thread in threads:
                    thread.join(timeout=5)

    def test_cluster_join_three_hosts_and_retry(self):
        with ExitStack() as stack:
            servers = [
                make_server('127.0.0.1', 0, stack.enter_context(tempfile.TemporaryDirectory())) for _ in range(3)
            ]
            threads = [threading.Thread(target=server.serve_forever, daemon=True) for server in servers]
            for thread in threads:
                thread.start()
            try:
                first, second, third = [server.service.hosts for server in servers]
                seed = {'endpoint': second.endpoint, 'token': second.token}
                # Exercise the browser-facing Add host route, not just the directory helper.
                with urlopen(
                    Request(
                        first.endpoint + '/api/hosts/add',
                        data=json.dumps(seed).encode(),
                        headers={'Content-Type': 'application/json'},
                    ),
                    timeout=10,
                ) as response:
                    self.assertEqual(response.status, 201)
                self.assertEqual([r['id'] for r in second.list()], [first.id])
                third.join(seed)
                for directory in (first, second, third):
                    self.assertEqual(
                        {r['id'] for r in directory.list()}, {first.id, second.id, third.id} - {directory.id}
                    )
                    self.assertTrue(all('token' not in r for r in directory.list()))
                    self.assertEqual(len(hosts.HostDirectory(directory.root).list()), 2)
                # Repeating Add is idempotent; partial fan-out can be retried safely.
                original = hosts.cluster_request

                def fail_merge(record, operation, members=None):
                    if record['id'] == third.id and operation == 'merge':
                        raise BenchmarkError('offline')
                    return original(record, operation, members)

                with mock.patch.object(hosts, 'cluster_request', side_effect=fail_merge):
                    with self.assertRaisesRegex(BenchmarkError, 'partially applied'):
                        first.join(seed)
                first.join(seed)
                self.assertEqual(len(first.list()), 2)
                # Equal run/comparison ids on different machines must remain distinct.
                for server in servers:
                    stack.enter_context(
                        mock.patch.object(server.service, 'run_list', return_value=[{'id': 'same-run'}])
                    )
                    stack.enter_context(
                        mock.patch.object(
                            server.service,
                            'local_ydb_comparison',
                            return_value={'entries': [{'run': 'same-run', 'profile': 'same-profile'}]},
                        )
                    )
                    stack.enter_context(
                        mock.patch.object(
                            server.service,
                            'saved_comparisons',
                            return_value=[
                                {
                                    'id': 'same-comparison',
                                    'profiles': [['same-run', 'same-profile']],
                                    'baseline': ['same-run', 'same-profile'],
                                }
                            ],
                        )
                    )
                federation = Federation(servers[0].service)
                runs = federation.runs({})
                self.assertEqual(len({run['id'] for run in runs['entries']}), 3)
                self.assertEqual(runs['errors'], [])
                self.assertEqual(len(federation.runs({}, second.id)['entries']), 1)
                comparisons = federation.comparisons()['entries']
                self.assertEqual(len({record['id'] for record in comparisons}), 3)
                for record in comparisons:
                    self.assertEqual(record['profiles'][0][0], reference(record['host_id'], 'same-run'))
                profiles = federation.profiles([reference(first.id, 'same-run'), reference(second.id, 'same-run')])
                self.assertEqual(len({row['run'] for row in profiles['entries']}), 2)
                self.assertEqual(profiles['errors'], [])
                with urlopen(first.endpoint + '/api/federation/runs', timeout=10) as response:
                    self.assertEqual(len(json.load(response)['entries']), 3)
                with mock.patch.object(federation, 'read', side_effect=BenchmarkError('offline')):
                    unavailable = federation.runs({})
                    self.assertEqual(len(unavailable['errors']), 3)
                    self.assertEqual(unavailable['entries'], [])
                with self.assertRaises(HTTPError) as error:
                    urlopen(
                        Request(
                            first.endpoint + '/peer/cluster/snapshot',
                            data=b'{}',
                            headers={'Content-Type': 'application/json'},
                        ),
                        timeout=5,
                    )
                self.assertEqual(error.exception.code, 401)
                with self.assertRaises(HTTPError) as error:
                    urlopen(
                        Request(
                            first.endpoint + '/peer/cluster/snapshot',
                            data=b'{}',
                            headers={
                                'Content-Type': 'application/json',
                                'Origin': first.endpoint,
                                'Authorization': 'Bearer ' + first.token,
                            },
                        ),
                        timeout=5,
                    )
                self.assertEqual(error.exception.code, 403)
            finally:
                for server in servers:
                    server.shutdown()
                    server.server_close()
                for thread in threads:
                    thread.join(timeout=5)

    def test_cluster_rejects_conflict_without_changing_directory(self):
        with tempfile.TemporaryDirectory() as output:
            directory = hosts.HostDirectory(output)
            directory.endpoint, directory.port = 'http://127.0.0.1:31999', 31999
            record = directory.snapshot()[0]
            with self.assertRaisesRegex(BenchmarkError, 'conflicting'):
                directory.merge([{**record, 'token': 'x' * 32}])
            self.assertEqual(directory.list(), [])
            self.assertFalse(directory.path.exists())

    def test_http_peer_and_proxy(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            servers = [make_server('127.0.0.1', 0, output) for output in (first, second)]
            threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in servers]
            for thread in threads:
                thread.start()
            try:
                local, peer = servers
                local_url = 'http://127.0.0.1:{}'.format(local.server_port)
                request = Request(
                    local_url + '/api/hosts/token',
                    data=b'{}',
                    headers={'Content-Type': 'application/json', 'Origin': local_url},
                )
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(response.headers['Cache-Control'], 'no-store')
                    self.assertEqual(json.load(response), {'token': local.service.hosts.token})
                request.headers['Origin'] = 'https://other.example'
                with self.assertRaises(HTTPError) as error:
                    urlopen(request, timeout=5)
                self.assertEqual(error.exception.code, 403)
                peer_url = 'http://127.0.0.1:{}'.format(peer.server_port)
                with self.assertRaises(HTTPError) as error:
                    urlopen(peer_url + '/peer/api/runs', timeout=5)
                self.assertEqual(error.exception.code, 401)
                record = local.service.hosts.add({'endpoint': peer_url, 'token': peer.service.hosts.token})
                self.assertEqual(record['port'], peer.server_port)
                with urlopen('http://127.0.0.1:{}/api/hosts'.format(local.server_port), timeout=5) as response:
                    self.assertEqual(json.load(response)['local']['port'], local.server_port)
                base = 'http://127.0.0.1:{}/api/hosts/{}/api/'.format(local.server_port, record['id'])
                with urlopen(base + 'runs', timeout=5) as response:
                    self.assertEqual(json.load(response), [])
                with self.assertRaises(HTTPError) as error:
                    urlopen(Request(base + 'runs', data=b'{}'), timeout=5)
                self.assertEqual(error.exception.code, 403)
                with self.assertRaises(HTTPError) as error:
                    urlopen(base + 'hosts', timeout=5)
                self.assertEqual(error.exception.code, 502)
                with self.assertRaises(HTTPError) as error:
                    urlopen(base + 'hosts/token', timeout=5)
                self.assertEqual(error.exception.code, 502)
                with self.assertRaises(HTTPError) as error:
                    urlopen(Request(base + 'hosts/token', data=b'{}'), timeout=5)
                self.assertEqual(error.exception.code, 403)
                with self.assertRaises(HTTPError) as error:
                    urlopen(
                        Request(
                            peer_url + '/peer/api/hosts/token',
                            headers={'Authorization': 'Bearer ' + peer.service.hosts.token},
                        ),
                        timeout=5,
                    )
                self.assertEqual(error.exception.code, 403)
            finally:
                for server in servers:
                    server.shutdown()
                    server.server_close()
                for thread in threads:
                    thread.join(timeout=5)

    def test_identity_persistence_and_secret(self):
        with tempfile.TemporaryDirectory() as output:
            first = hosts.HostDirectory(output)
            second = hosts.HostDirectory(output)
            self.assertEqual(first.id, second.id)
            self.assertEqual(first.token, second.token)
            self.assertTrue(first.authorized('Bearer ' + first.token))
            self.assertFalse(first.authorized(None))
            self.assertEqual((Path(output) / '.peer-token').stat().st_mode & 0o777, 0o600)

    def test_endpoint_and_routes(self):
        host_id = str(uuid.uuid4())
        self.assertEqual(split_reference(reference(host_id, 'same-run'), 'local'), (host_id, 'same-run'))
        self.assertEqual(split_reference('legacy-run', host_id), (host_id, 'legacy-run'))
        for endpoint in (
            'file:///tmp/a',
            'http://127.0.0.1:80/a',
            'http://user:pass@127.0.0.1:80',
            'http://127.0.0.1:abc',
        ):
            with self.subTest(endpoint=endpoint), self.assertRaises(BenchmarkError):
                hosts.validate_endpoint(endpoint)
        self.assertEqual(hosts.validate_endpoint('https://example.com:443/'), 'https://example.com:443')
        for endpoint in ('http://sas8-6910.search.yandex.net:42415', 'http://10.0.0.1:42415', 'http://[::1]:42415'):
            self.assertEqual(hosts.validate_endpoint(endpoint), endpoint)
        self.assertTrue(hosts.allowed_path('/api/runs/a/config'))
        for path in ('//example.com/api/runs', '/api/hosts', '/peer/api/runs', '/api/import'):
            self.assertFalse(hosts.allowed_path(path))

    def test_add_remove_and_redaction(self):
        with tempfile.TemporaryDirectory() as output:
            directory = hosts.HostDirectory(output)
            host_id = str(uuid.uuid4())
            reply = (200, 'application/json', json.dumps({'id': host_id, 'protocol': 1, 'name': 'peer'}).encode())
            with mock.patch.object(hosts, 'request_peer', return_value=reply):
                record = directory.add({'endpoint': 'http://127.0.0.1:42420', 'token': 'x' * 32})
                self.assertNotIn('token', record)
                self.assertNotIn('token', directory.list()[0])
                with self.assertRaises(BenchmarkError):
                    directory.add({'endpoint': 'http://127.0.0.1:42420', 'token': 'x' * 32})
            self.assertEqual(len(hosts.HostDirectory(output).list()), 1)
            directory.remove(host_id)
            self.assertEqual(hosts.HostDirectory(output).list(), [])
