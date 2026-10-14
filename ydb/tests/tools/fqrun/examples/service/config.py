"""Generate a local fqrun setup using the checked-in WASM test modules."""
import json
from pathlib import Path


def app_config(source_root, batch_rows=1, timeout_ms=30000):
    root = Path(source_root)
    modules = []
    for name in ('profile', 'echo'):
        directory = root / 'ydb/udfs/wasm' / name
        modules.append('Modules { ModulePath: %s ManifestPath: %s }' % (
            json.dumps(str(directory / 'ut/data' / (name + '.wasm'))),
            json.dumps(str(directory / 'manifest.json')),
        ))
    wasm = '\nWasmServices { Enabled: true %s MaxBatchRows: %d CallTimeoutMs: %d }\n' % (
        '\n'.join(modules), batch_rows, timeout_ms,
    )
    config = (root / 'ydb/tests/tools/fqrun/configuration/app_config.conf').read_text()
    return config.replace('FederatedQueryConfig {', 'FederatedQueryConfig {' + wasm, 1)


def connection(name, endpoint, protocol='HTTP', certificate='', token='fqrun-example-token', tvm=False):
    # --connection accepts ConnectionContent directly, without a content wrapper.
    auth = 'tvm_auth { source_id: 2020001 destination_id: 2020002 secret: "fqrun-example-tvm-secret" }' if tvm else \
        'auth { token { token: %s } }' % json.dumps(token)
    return '''name: %s
acl { visibility: PRIVATE }
setting { external_service {
  protocol: %s endpoint: %s insecure: %s
  %s
  ca_certificate: %s
} }
''' % (json.dumps(name), protocol, json.dumps(endpoint),
       'false' if certificate else 'true', auth, json.dumps(certificate))


def connection_ddl(name, endpoint, protocol='HTTP', certificate='', auth='TOKEN'):
    properties = dict(SOURCE_TYPE='ExternalService', PROTOCOL=protocol, LOCATION=endpoint,
                      AUTH_METHOD=auth)
    if auth == 'TOKEN':
        properties['TOKEN_SECRET_NAME'] = 'example_token'
    elif auth == 'TVM':
        properties.update(TVM_SOURCE_ID='2020001', TVM_DESTINATION_ID='2020002', TVM_SECRET_NAME='example_tvm_secret')
    if certificate:
        properties['CA_CERTIFICATE'] = certificate
    entries = ['    %s = %s' % (key, json.dumps(value)) for key, value in properties.items()]
    entries.append('    USE_TLS = %s' % ('TRUE' if certificate else 'FALSE'))
    return 'CREATE EXTERNAL DATA SOURCE `%s` WITH (\n%s\n);\n' % (name, ',\n'.join(entries))


def profile_query(alias, ids=(42, 43)):
    source = ' UNION ALL '.join('SELECT %dul AS id' % value for value in ids)
    return '''$input = %s;
$profiles = PROCESS $input USING EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')
    WITH CONNECTION='%s', INPUT_TYPE=Struct<id:Uint64>,
         OUTPUT_TYPE=Struct<id:Uint64,name:Utf8,score:Uint32>;
SELECT id, name, score FROM $profiles ORDER BY id;
''' % (source, alias)


def echo_query():
    return '''$input = SELECT "hello" AS message;
$echo = PROCESS $input USING EXTERNAL FUNCTION('Echo', 'Echo')
    WITH CONNECTION='echo_http', INPUT_TYPE=Struct<message:String>,
         OUTPUT_TYPE=Struct<value:String,length:Uint64,empty:Bool,delta:Int64>;
SELECT * FROM $echo;
'''


def write_setup(source_root, output_dir, service, batch_rows=1, timeout_ms=30000):
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / 'app.conf').write_text(app_config(source_root, batch_rows, timeout_ms))
    host = 'localhost' if service.tls else '127.0.0.1'
    http = '%s://%s:%d' % ('https' if service.tls else 'http', host, service.http.server_port)
    specs = (
        ('profiles_http', http, 'HTTP'),
        ('profiles_grpc', '%s:%d' % (host, service.grpc_port), 'GRPC'),
        ('echo_http', http, 'HTTP'),
    )
    ddl = []
    for name, endpoint, protocol in specs:
        ddl.append(connection_ddl(name, endpoint, protocol, service.ca_certificate,
                                  auth='TVM' if service.require_tvm_ticket else 'TOKEN'))
        (output / (name + '.pb.txt')).write_text(connection(
            name, endpoint, protocol, service.ca_certificate, tvm=service.require_tvm_ticket))
    for alias in ('profiles_http', 'profiles_grpc'):
        (output / (alias + '.sql')).write_text(profile_query(alias))
    (output / 'echo.sql').write_text(echo_query())
    (output / 'connections.sql').write_text('\n'.join(ddl))
    secrets = {'example_tvm_secret': 'fqrun-example-tvm-secret'} if service.require_tvm_ticket else \
        {'example_token': 'fqrun-example-token'}
    (output / 'secrets.json').write_text(json.dumps(secrets))
    (output / 'secrets.json').chmod(0o600)
