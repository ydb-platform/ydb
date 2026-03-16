import testsuite

# Common testsuite plugins
pytest_plugins = [
    'testsuite.plugins.asyncio',
    'testsuite.daemons.pytest_plugin',
    'testsuite.environment.pytest_plugin',
    'testsuite.mockserver.pytest_plugin',
    'testsuite.plugins.assertrepr_compare',
    'testsuite.plugins.asyncexc',
    'testsuite.plugins.colors',
    'testsuite.plugins.common',
    'testsuite.plugins.matching',
    'testsuite.plugins.misc',
    'testsuite.plugins.mocked_time',
    'testsuite.plugins.network',
    'testsuite.plugins.object_hook',
    'testsuite.plugins.servicelogs',
    'testsuite.plugins.servicetest',
    'testsuite.plugins.tcp_mockserver',
    'testsuite.plugins.testpoint',
    'testsuite.plugins.tracing',
    'testsuite.plugins.verify_file_paths',
]


def pytest_report_header(config):
    return [f'yandex-taxi-testsuite: version {testsuite.__version__}']
