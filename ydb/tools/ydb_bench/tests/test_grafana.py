import json
import shutil
import subprocess
import unittest
from unittest import mock

from ydb.tools.ydb_bench.lib import grafana, monitoring_settings_ui, web
from ydb.tools.ydb_bench.lib.common import BenchmarkError


class GrafanaTest(unittest.TestCase):
    def setUp(self):
        self.settings = mock.Mock()
        self.settings.snapshot.return_value = dict(
            owner_id='a',
            revision=1,
            settings=dict(
                grafana_url='http://browser/grafana',
                grafana_api_url='http://internal',
                grafana_token='secret',
                grafana_datasource_uid='prometheus',
            ),
        )
        self.hosts = mock.Mock(id='a')
        self.client = grafana.Grafana(self.settings, self.hosts)

    def test_bundle_scopes_all_queries_and_retains_unique_uids(self):
        for name in grafana.BUNDLES:
            result = grafana.dashboard(name, 'prometheus')
            self.assertEqual(grafana.bundle_uid(name), result['uid'])
            self.assertLessEqual(len(result['uid']), 40)
            self.assertIsNone(result['id'])
            self.assertEqual('', result['refresh'])
            expressions = []

            def visit(item):
                if isinstance(item, dict):
                    if 'expr' in item:
                        expressions.append(item['expr'])
                    for child in item.values():
                        visit(child)
                elif isinstance(item, list):
                    for child in item:
                        visit(child)

            visit(result['panels'])
            self.assertTrue(expressions)
            for expression in expressions:
                if expression in ('', 'vector(1)'):
                    continue
                self.assertIn('bench_attempt=~"${bench_attempt:regex}"', expression)
                self.assertIn('bench_run=~"${bench_run:regex}"', expression)
                self.assertNotIn('database="$database"', expression)
            variables = {v['name']: v for v in result['templating']['list']}
            self.assertEqual('prometheus', variables['ds']['current']['value'])
            if 'database' in variables:
                self.assertTrue(variables['database']['includeAll'])

    def test_quoted_braces_are_not_selectors(self):
        value = grafana.scoped_expression('sum(x{label=~"x{1,2}"})')
        self.assertIn('label=~"x{1,2}"', value)
        self.assertEqual(1, value.count('bench_host='))

    def test_bare_metrics_and_grouping(self):
        value = grafana.scoped_expression('sum by(le) (rate(tablets_total[$__rate_interval])) / scalar(count(x{}))')
        self.assertEqual(2, value.count('bench_host='))
        self.assertIn('sum by(le) (rate(tablets_total{', value)
        self.assertIn('[$__rate_interval]', value)
        self.assertEqual('vector(1)', grafana.scoped_expression('vector(1)'))

    def test_catalog_contains_more_than_original_two(self):
        self.assertIn('distributed-storage-self-heal', grafana.BUNDLES)
        self.assertIn('topic-consumer', grafana.BUNDLES)

    def test_catalog_merges_bundles_and_redacts_datasource_details(self):
        with mock.patch.object(
            self.client,
            '_request',
            side_effect=[
                [dict(uid='external', title='<Dashboard>'), dict(uid='ydb-bench-actors-v1', title='Actors')],
                [
                    dict(uid='p', name='Prometheus', type='prometheus', password='secret'),
                    dict(uid='l', name='Loki', type='loki'),
                ],
            ],
        ):
            result = self.client.execute('catalog')
        self.assertEqual(len(grafana.BUNDLES) + 1, len(result['dashboards']))
        self.assertEqual([dict(uid='p', name='Prometheus')], result['datasources'])
        self.assertNotIn('secret', json.dumps(result))

    def test_install_does_not_overwrite(self):
        options = dict(bundle='actors', datasource_uid='p', revision=1)
        with mock.patch.object(self.client, '_request', return_value={'dashboard': {}}) as request:
            with self.assertRaisesRegex(BenchmarkError, 'already exists'):
                self.client.execute('install', options)
            self.assertEqual(1, request.call_count)
        with mock.patch.object(self.client, '_request', side_effect=[None, dict(type='prometheus'), {}]) as request:
            self.client.execute('install', options)
            self.assertFalse(request.call_args.args[2]['overwrite'])
        with self.assertRaisesRegex(BenchmarkError, 'changed'):
            self.client.execute('install', dict(options, revision=0))

    def test_owner_forwarding_and_loop_guard(self):
        self.settings.snapshot.return_value['owner_id'] = 'b'
        with mock.patch.object(grafana, 'exchange', return_value={}) as exchange:
            self.client.execute('catalog')
            self.assertEqual('grafana', exchange.call_args.args[1])
            with self.assertRaises(BenchmarkError):
                self.client.execute('catalog', forwarded=True)

    def test_http_errors_do_not_leak_response_or_token(self):
        with mock.patch.object(grafana, 'build_opener') as opener:
            opener.return_value.open.side_effect = grafana.HTTPError('http://internal', 403, 'secret', {}, None)
            with self.assertRaises(BenchmarkError) as error:
                self.client.execute('catalog')
            self.assertNotIn('secret', str(error.exception))
            self.assertIn('403', str(error.exception))

    @unittest.skipUnless(shutil.which('node'), 'node is required for dashboard link checks')
    def test_config_cache_and_stable_reservation_button(self):
        script = monitoring_settings_ui.JS
        script += web._JS[
            web._JS.index('function renderLocalYdbProfile(') : web._JS.index('async function mountLocalYdbProfile(')
        ]
        script += r'''
const assert=require('assert');
let now=1000,requests=[],opened;
Date.now=()=>now;
api=path=>{assert.equal(path,'/api/grafana/config');return new Promise((resolve,reject)=>requests.push({resolve,reject}))};
class Element{
  constructor(){this.isConnected=true;this.children=[];this.dataset={}}
  querySelector(){return this.children[0]||null}
  replaceChildren(){this.children=[]}
  append(value){this.children.push(value)}
}
document={createElement:()=>new Element(),querySelector:()=>({remove(){}})};
showGrafanaChooser=(...args)=>opened=args;
esc=String;displayError=String;savedYdbConfigurationHtml=()=>'';
const tick=()=>new Promise(resolve=>setImmediate(resolve));
(async()=>{
  const first=new Element(),second=new Element();
  let a=mountGrafana(first,'run',{profile:'a'},1,2),b=mountGrafana(second,'run',{profile:'b'},1,2);
  assert.equal(requests.length,1);
  requests[0].resolve({configured:true,local_id:'host'});await Promise.all([a,b]);
  const button=first.children[0];
  for(let i=0;i<5;i++)await mountGrafana(first,'run',{profile:'a'},1,10+i);
  assert.equal(first.children[0],button);assert.equal(requests.length,1);
  button.onclick();assert.equal(opened[4],14);
  now+=30001;a=mountGrafana(first,'run',{profile:'a'},1,20);
  assert.equal(first.children[0],button);assert.equal(requests.length,2);
  requests[1].reject(Error('temporary'));await a;
  assert.equal(first.children[0],button);
  await mountGrafana(first,'run',{},1,21);assert.equal(requests.length,2);
  invalidateGrafanaConfig();a=mountGrafana(first,'run',{},1,22);
  requests[2].resolve({configured:false});await a;
  assert.equal(first.children.length,0);
  await mountGrafana(first,'run',{},1,23);assert.equal(requests.length,3);
  invalidateGrafanaConfig();a=mountGrafana(first,'run',{},1,24);
  invalidateGrafanaConfig();b=mountGrafana(first,'run',{},1,25);
  requests[4].resolve({configured:false});await b;
  requests[3].resolve({configured:true});await a;
  assert.equal(first.children.length,0);
  invalidateGrafanaConfig();a=mountGrafana(first,'run',{},1,26);
  requests[5].reject(Error('offline'));await a;
  await mountGrafana(first,'run',{},1,27);assert.equal(requests.length,6);
  now+=10001;a=mountGrafana(first,'run',{},1,28);
  requests[6].resolve({configured:true,local_id:'host'});await a;
  assert.equal(first.children.length,1);
  const detached=new Element();detached.isConnected=false;
  await mountGrafana(detached,'run',{},1,2);assert.equal(detached.children.length,0);
  await mountGrafana(first,'run',{},NaN,2);assert.equal(first.children.length,0);
  const container={dataset:{localYdbRunId:'run'},parts:{},rebuilds:0,
    querySelector(key){return this.parts[key]||null},
    set innerHTML(value){this.rebuilds++;this.parts={
      '[data-reservation-body]':{},'[data-reservation-status]':{},'[data-reservation-grafana]':new Element()}}};
  const data={parameters:{mode:'deploy'},state:'running',progress:{phase:'cluster-ready'},
    benchmark:'dedicated-ydb',profile:'test',started_at:'2026-09-28T00:00:00Z'};
  renderLocalYdbProfile(container,data);await tick();
  const control=container.parts['[data-reservation-grafana]'].children[0];assert(control);
  for(let i=0;i<5;i++){renderLocalYdbProfile(container,data);await tick()}
  assert.equal(container.rebuilds,1);
  assert.equal(container.parts['[data-reservation-grafana]'].children[0],control);
  assert.equal(requests.length,7);
})().catch(error=>{console.error(error);process.exitCode=1});
'''
        subprocess.run([shutil.which('node'), '-e', script], check=True, capture_output=True, timeout=10)

    @unittest.skipUnless(shutil.which('node'), 'node is required for dashboard link checks')
    def test_link_context_and_no_sample_count(self):
        script = monitoring_settings_ui.JS.split('async function mountGrafana', 1)[0]
        script = "const runDisplay=value=>value.split(':').pop();\n" + script + r'''
const assert=require('assert');
const url=new URL(grafanaLink('https://example/grafana','uid','host:run','host',
  {benchmark:'distributed-ydb',profile:'a/b & c',attempt:'verification'},1000.2,2000.2,'p',true));
assert.equal(url.pathname,'/grafana/d/uid');
assert.equal(url.searchParams.get('var-bench_profile'),'a/b & c');
assert.equal(url.searchParams.get('var-bench_run'),'run');
assert.equal(url.searchParams.get('var-bench_attempt'),'verification');
assert.equal(url.searchParams.get('from'),'1000');
assert.equal(url.searchParams.get('to'),'2001');
const deployment=new URL(grafanaLink('https://example/grafana','uid','host:run','host',
  {benchmark:'distributed-ydb',profile:'reservation',attempt:'deployment'},1000,61000,'p',true));
assert.equal(deployment.searchParams.get('var-bench_attempt'),'deployment');
assert.equal(deployment.searchParams.get('var-bench_profile'),'reservation');
assert.equal(deployment.searchParams.get('to'),'61000');
'''
        subprocess.run([shutil.which('node'), '-e', script], check=True, capture_output=True, timeout=10)
        self.assertNotIn("value.samples+' / '", monitoring_settings_ui.JS)
