"""Configure database connection for tests."""

from os import environ, path

tests_path = path.dirname(__file__)
conf_file = environ.get('TESTDB', 'default.cnf')
conf_path = path.join(tests_path, conf_file)
connect_kwargs = dict(
    read_default_file = conf_path,
    read_default_group = "MySQLdb-tests",
)


def connection_kwargs(kwargs):
    db_kwargs = connect_kwargs.copy()
    db_kwargs.update(kwargs)
    return db_kwargs


def connection_factory(**kwargs):
    import MySQLdb
    db_kwargs = connection_kwargs(kwargs)
    db = MySQLdb.connect(**db_kwargs)
    return db
