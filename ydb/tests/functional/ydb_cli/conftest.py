# XXX: setting of pytest_plugins should work if specified directly in test modules
# but somehow it does not
#
# for ydb_{cluster, database, ...} fixture family
pytest_plugins = 'ydb.tests.library.harness.ydb_fixtures'
