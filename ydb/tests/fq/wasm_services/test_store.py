"""Real ydbd/UDF Store integration; no local module paths in FQ configuration."""
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2

import grpc
import pytest
import yatest.common
import ydb

from ydb.public.api.grpc.ydb_udf_v1_pb2_grpc import UdfServiceStub
from ydb.public.api.protos import ydb_udf_pb2 as udf
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.public.api.protos.draft import fq_pb2 as fq
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_port_allocator import KikimrPortManagerPortAllocator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.tools.fq_runner.fq_client import FederatedQueryClient
from test_query import query, start


def wait_for_store_storage(endpoint, database):
    # Tenant readiness precedes Store table creation. Use the same read-only
    # SQL readiness check as the functional UDF Store tests, for both tables
    # written by UploadModule. Do not retry potentially partial uploads.
    with ydb.Driver(endpoint='grpc://' + endpoint, database=database) as driver:
        driver.wait(timeout=30)
        with ydb.QuerySessionPool(driver, size=1) as pool:
            deadline = time.monotonic() + 90
            while True:
                try:
                    pool.execute_with_retries(
                        f'SELECT COUNT(*) FROM `{database}/.metadata/udf_store/modules`;'
                        f'SELECT COUNT(*) FROM `{database}/.metadata/udf_store/module_chunks`;'
                    )
                    return
                except ydb.SchemeError as error:
                    assert time.monotonic() < deadline, 'UDF Store initialization did not finish: ' + str(error)
                    time.sleep(0.5)


def trace_attrs(span):
    return {a.key: a.value.string_value for a in span.attributes}


@pytest.fixture
def trace_collector():
    class Collector:
        def __init__(self):
            self.spans = []
            self.lock = threading.Lock()

        def export(self, request, context):
            with self.lock:
                for resource in request.resource_spans:
                    for scope in resource.scope_spans:
                        self.spans.extend(scope.spans)
            return b''

        def query_spans(self, query_id):
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                with self.lock:
                    roots = [s for s in self.spans if s.name == 'SQL execution' and trace_attrs(s).get('query_id') == query_id]
                    if roots:
                        spans = [s for s in self.spans if s.trace_id == roots[-1].trace_id]
                        if any(s.name == 'External call' for s in spans):
                            return roots[-1], spans
                time.sleep(0.05)
            pytest.fail('SQL trace did not reach OTLP collector: ' + query_id)

    collector = Collector()
    server = grpc.server(ThreadPoolExecutor(max_workers=2))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
        'opentelemetry.proto.collector.trace.v1.TraceService', {
            'Export': grpc.unary_unary_rpc_method_handler(
                collector.export,
                request_deserializer=trace_service_pb2.ExportTraceServiceRequest.FromString,
                response_serializer=lambda response: response,
            ),
        }),))
    collector.server = server
    collector.port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    try:
        yield collector
    finally:
        server.stop(0).wait()


@pytest.fixture
def store_server(trace_collector):
    ports = KikimrPortManagerPortAllocator()
    config = KikimrConfigGenerator(
        port_allocator=ports, extra_feature_flags=['enable_wasm_compile_controller'],
        extra_grpc_services=['udf', 'yq', 'yq_private'],
    )
    config.yaml_config['tracing_config'] = {
        'backend': {'opentelemetry': {'collector_url': f'grpc://127.0.0.1:{trace_collector.port}', 'service_name': 'wasm-store-test'}},
        'uploader': {'max_batch_accumulation_milliseconds': 10, 'max_exported_spans_per_second': 10000, 'span_export_timeout_seconds': 5},
    }
    config.yaml_config.setdefault('query_service_config', {})['available_external_data_sources'] = ['ExternalService']
    config.yaml_config['feature_flags']['enable_external_data_sources'] = True
    config.yaml_config['feature_flags']['enable_schema_secrets'] = True
    config.yaml_config['udf_store_config'] = {'enabled': True, 'enable_wasm_udf': True, 'kv_storage_media': 'hdd'}
    cluster = KiKiMR(configurator=config)
    cluster.start()
    try:
        database = '/Root/services'
        cluster.create_database(database, storage_pool_units_count={'hdd': 1})
        endpoint = f'localhost:{ports.get_slot_port_allocator(1).grpc_port}'
        # The static node is already running with FQ disabled. Only the tenant
        # node receives this configuration, keeping FQ and its Store together.
        settings = config.yaml_config['federated_query_config']
        settings.update(enabled=True, enable_task_counters=True)
        settings['wasm_services'] = {'enabled': True, 'call_timeout_ms': 180000, 'trace_sample_per_million': 1000000}  # UDF_STORE is the default.
        settings['common']['keep_internal_errors'] = True
        settings['control_plane_storage'].update(
            available_connection=['EXTERNAL_SERVICE'], super_users=['root@builtin'],
            retry_policy_mapping=[{'status_code': list(range(2, 14)), 'policy': {'retry_count': 0}}],  # Surface the incompatible replacement without whole-query retries.
        )
        for name in ('control_plane_storage', 'db_pool'):
            settings[name]['storage'] = {'endpoint': endpoint, 'database': database, 'table_prefix': 'fq/' + name}
        settings['checkpoint_coordinator']['enabled'] = False
        settings['private_api'].update(loopback=True, task_service_endpoint=endpoint, task_service_database=database)
        settings['private_proxy']['granted_users'] = ['root@builtin']
        settings['nodes_manager'].update(host='localhost', port=ports.get_slot_port_allocator(1).grpc_port)
        settings['token_accessor']['enabled'] = False
        settings['gateways']['dq']['default_settings'] = [{'name': 'ComputeActorType', 'value': 'sync'}]
        settings['pinger']['ping_period'] = '1s'
        settings['compute'] = {'default_compute': 'IN_PLACE'}
        config.write_proto_configs(cluster.config_path)
        cluster.register_and_start_slots(database, count=1)
        cluster.wait_tenant_up(database)
        wait_for_store_storage(endpoint, database)
        yield endpoint, database, next(iter(cluster.slots.values())).mon_port
    finally:
        cluster.stop()


def upload(stub, metadata, manifest, body, expected_status=StatusIds.SUCCESS):
    chunks = [udf.UploadModuleChunk(metadata=udf.UploadModuleMetadata(
        params=udf.UploadModuleParams(manifest_json=json.dumps(manifest)), total_size=len(body),
    )), udf.UploadModuleChunk(data=body)]
    responses = list(stub.UploadModule(iter(chunks), metadata=metadata, timeout=60))
    assert len(responses) == 1
    assert responses[0].operation.status == expected_status, responses[0]


def wait_for_module(stub, metadata, name):
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        response = stub.DescribeModule(udf.DescribeModuleRequest(name=name), metadata=metadata, timeout=15)
        result = udf.DescribeModuleResult()
        assert response.operation.result.Unpack(result), response
        assert all(platform.status != udf.FAILED for platform in result.platforms), result
        if result.platforms and all(platform.status == udf.READY for platform in result.platforms):
            return
        time.sleep(0.5)
    pytest.fail('UDF Store compilation did not finish')


def await_profile(client, alias):
    # Artifact readiness precedes asynchronous loading into the node catalog.
    deadline = time.monotonic() + 90
    while True:
        query_id = start(client, query(alias))
        client.wait_query_status(query_id, [fq.QueryMeta.COMPLETED, fq.QueryMeta.FAILED])
        description = client.describe_query(query_id).result.query
        if description.meta.status == fq.QueryMeta.COMPLETED:
            rows = client.get_result_data(query_id).result.result_set.rows
            assert len(rows) == 1
            assert rows[0].items[0].uint64_value == 42
            assert rows[0].items[1].text_value == 'Ada'
            assert rows[0].items[2].uint32_value == 97
            return query_id
        issues = str(description.issue)
        assert time.monotonic() < deadline, issues
        assert 'Unknown' in issues or 'not loaded' in issues or 'not found' in issues, issues
        time.sleep(0.5)


def test_service_only_store_module_http_and_grpc(store_server, services, trace_collector):
    endpoint, database, monitoring_port = store_server

    class DatabaseClient(FederatedQueryClient):
        def _create_meta(self):
            return super()._create_meta() + [('x-ydb-database', database)]

    client = DatabaseClient('my_folder', endpoint)
    try:
        client.create_external_service_connection('profiles_http', f'http://127.0.0.1:{services.http.server_port}',
                                                  insecure=True, token='host-secret')
        client.create_external_service_connection('profiles_grpc', f'127.0.0.1:{services.grpc_port}',
                                                  protocol=fq.ExternalService.GRPC, insecure=True, token='host-secret')
        # Upload after FQ/transport factory startup, using the existing API and compiler.
        with open(yatest.common.source_path('ydb/udfs/wasm/profile/manifest.json')) as source:
            manifest = json.load(source)
        with open(yatest.common.source_path('ydb/udfs/wasm/profile/ut/data/profile.wasm'), 'rb') as source:
            body = source.read()
        metadata = (('x-ydb-database', database),)
        with grpc.insecure_channel(endpoint) as channel:
            stub = UdfServiceStub(channel)
            invalid_manifest = dict(manifest, service_abi_version=1)
            upload(stub, metadata, invalid_manifest, body, StatusIds.BAD_REQUEST)
            upload(stub, metadata, manifest, body)
            wait_for_module(stub, metadata, manifest['module_name'])
            query_ids = [await_profile(client, 'profiles_http'), await_profile(client, 'profiles_grpc')]
            trace_ids = set()
            for query_id, protocol in zip(query_ids, ('http', 'grpc')):
                root, spans = trace_collector.query_spans(query_id)
                trace_ids.add(root.trace_id)
                by_id = {s.span_id: s for s in spans}
                calls = [s for s in spans if s.name == 'External call']
                assert len(calls) == 1, spans
                call = calls[0]
                assert trace_attrs(call)['call_alias'] == 'profile.lookup'
                assert trace_attrs(call)['protocol'] == protocol
                assert trace_attrs(call)['outcome'] == 'success'
                for name in ('WASM invocation', 'WASM transform', 'ComputeActor', 'SQL execution'):
                    call = by_id[call.parent_span_id]
                    assert call.name == name, spans
                assert call.span_id == root.span_id
                assert not any('host-secret' in str(s) for s in spans)
            assert len(trace_ids) == 2
            # The remaining Store replacement/pending-call checks run with an
            # unavailable exporter. Query progress must not depend on tracing.
            trace_collector.server.stop(0).wait()
            # ydbd Store modules use the same instrumented transport as fqrun.
            from urllib.request import urlopen
            with urlopen(f'http://localhost:{monitoring_port}/counters/counters=yq/json', timeout=10) as response:
                sensors = json.load(response)['sensors']
            for protocol in ('http', 'grpc'):
                completed = [s['value'] for s in sensors if all(s['labels'].get(k) == v for k, v in
                             dict(subsystem='wasm_services', component='requests', protocol=protocol,
                                  call_alias='profile.lookup', sensor='RequestsCompleted', outcome='success').items())]
                assert len(completed) == 1 and completed[0] >= 1, sensors
            services.delay_timeout = 120
            pending = start(client, query('profiles_http', id=999))
            assert services.started.wait(30), 'Old transform did not reach the service'
            # Change the contract while an old compartment is awaiting I/O.
            # Ada no longer fits the new schema; there is no version pinning.
            manifest['service_methods'][0]['output'][1]['max_bytes'] = 2
            upload(stub, metadata, manifest, body)
            wait_for_module(stub, metadata, manifest['module_name'])
            deadline = time.monotonic() + 60
            while True:
                current = start(client, query('profiles_http'))
                status = client.wait_query_status(current, [fq.QueryMeta.COMPLETED, fq.QueryMeta.FAILED])
                issues = str(client.describe_query(current).result.query.issue)
                if status == fq.QueryMeta.FAILED and 'Invalid WASM service result framing' in issues:
                    break
                assert time.monotonic() < deadline, 'Replacement not visible: ' + issues
                assert status == fq.QueryMeta.COMPLETED or 'Unknown' in issues or 'not loaded' in issues, issues
                time.sleep(0.5)
            # The already-created transform keeps its loaded code and contract.
            services.release.set()
            client.wait_query_status(pending, fq.QueryMeta.COMPLETED)
            old_rows = client.get_result_data(pending).result.result_set.rows
            assert len(old_rows) == 1 and old_rows[0].items[0].uint64_value == 999
            assert old_rows[0].items[1].text_value == 'Ada'
        assert {kind for kind, _, _ in services.requests} == {'http', 'grpc'}
        assert all(auth == 'Bearer host-secret' for _, _, auth in services.requests)
    finally:
        client.channel.close()


def test_kqp_service_store_http_and_grpc(store_server, services):
    endpoint, database, _ = store_server
    with open(yatest.common.source_path('ydb/udfs/wasm/profile/manifest.json')) as source:
        manifest = json.load(source)
    with open(yatest.common.source_path('ydb/udfs/wasm/profile/ut/data/profile.wasm'), 'rb') as source:
        body = source.read()
    metadata = (('x-ydb-database', database),)
    with grpc.insecure_channel(endpoint) as channel:
        stub = UdfServiceStub(channel)
        upload(stub, metadata, manifest, body)
        wait_for_module(stub, metadata, manifest['module_name'])
    with ydb.Driver(endpoint='grpc://' + endpoint, database=database) as driver:
        driver.wait(timeout=30)
        with ydb.QuerySessionPool(driver, size=1) as pool:
            pool.execute_with_retries("CREATE SECRET service_token WITH (value='host-secret');")
            for protocol, address in (('HTTP', f'http://127.0.0.1:{services.http.server_port}'),
                                      ('GRPC', f'127.0.0.1:{services.grpc_port}')):
                name = 'service_' + protocol.lower()
                pool.execute_with_retries(f"""CREATE EXTERNAL DATA SOURCE `{name}` WITH (
                    SOURCE_TYPE='ExternalService', LOCATION='{address}', PROTOCOL='{protocol}',
                    USE_TLS='false', AUTH_METHOD='TOKEN', TOKEN_SECRET_PATH='{database}/service_token');""")
                sql = f"""$input = SELECT 42ul AS id;
                $profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
                    WITH CONNECTION='{database}/{name}', INPUT_TYPE=Struct<id:Uint64>,
                         OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
                SELECT * FROM $profiles;"""
                deadline = time.monotonic() + 90
                while True:
                    before_calls = len(services.requests)
                    try:
                        result = pool.execute_with_retries(sql)
                        break
                    except ydb.Error as error:
                        message = str(error)
                        assert time.monotonic() < deadline, message
                        # Store READY precedes the node catalog update. KQP builds
                        # its gateway factory from that catalog on each compile.
                        pending_registration = 'Unregistered external function gateway type WASM_PROFILE' in message
                        assert 'Unknown' in message or 'not loaded' in message or pending_registration, message
                        assert len(services.requests) == before_calls, 'Readiness retry must not repeat an external call'
                        time.sleep(0.5)
                assert len(result[0].rows) == 1
                assert result[0].rows[0].id == 42
                assert result[0].rows[0].name == 'Ada'
                assert result[0].rows[0].score == 97
    assert {'http', 'grpc'} <= {kind for kind, _, _ in services.requests}
    assert all(auth == 'Bearer host-secret' for _, _, auth in services.requests)

    # KQP registers the common call metrics under its own counter group.
    from urllib.request import urlopen
    with urlopen(f'http://localhost:{store_server[2]}/counters/counters=kqp/json', timeout=10) as response:
        sensors = json.load(response)['sensors']
    for protocol in ('http', 'grpc'):
        completed = [s['value'] for s in sensors if all(s['labels'].get(k) == v for k, v in
                     dict(subsystem='wasm_services', component='requests', protocol=protocol,
                          call_alias='profile.lookup', sensor='RequestsCompleted', outcome='success').items())]
        assert len(completed) == 1 and completed[0] >= 1, sensors
