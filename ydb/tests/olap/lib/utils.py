import yatest.common
import os


def get_external_param(name: str, default):
    try:
        return yatest.common.get_param(name, default=default)
    except yatest.common.NoRuntimeFormed:
        return default


def external_param_is_true(name: str) -> bool:
    return get_external_param(name, '').lower() in ['t', 'true', 'yes', '1', 'da']


def parse_connection_string() -> tuple[str, str]:
    e = 'grpc://ydb-olap-testing-vla-0002.search.yandex.net:2135'
    d = 'olap-testing/kikimr/testing/acceptance-2'
    con_str = os.getenv('YDB_CONNECTION_STRING')
    if con_str:
        e, ps = con_str.split('?', 2)
        for p in ps.split('&'):
            k, d = p.split('=')
            if k == 'database':
                break
    e = get_external_param('ydb-endpoint', e)
    d = get_external_param('ydb-db', d).lstrip('/')
    return e if e.startswith('grpc') else f'grpc://{e}', d
