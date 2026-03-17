import itertools
import os
from collections.abc import Mapping, Sequence
from copy import copy

import sqlalchemy as sa
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import OperationalError, ProgrammingError

from ..utils import starts_with
from .orm import quote


def escape_like(string, escape_char='*'):
    """
    Escape the string parameter used in SQL LIKE expressions.

    ::

        from sqlalchemy_utils import escape_like


        query = session.query(User).filter(
            User.name.ilike(escape_like('John'))
        )


    :param string: a string to escape
    :param escape_char: escape character
    """
    return (
        string
        .replace(escape_char, escape_char * 2)
        .replace('%', escape_char + '%')
        .replace('_', escape_char + '_')
    )


def json_sql(value, scalars_to_json=True):
    """
    Convert python data structures to PostgreSQL specific SQLAlchemy JSON
    constructs. This function is extremly useful if you need to build
    PostgreSQL JSON on python side.

    .. note::

        This function needs PostgreSQL >= 9.4

    Scalars are converted to to_json SQLAlchemy function objects

    ::

        json_sql(1)     # Equals SQL: to_json(1)

        json_sql('a')   # to_json('a')


    Mappings are converted to json_build_object constructs

    ::

        json_sql({'a': 'c', '2': 5})  # json_build_object('a', 'c', '2', 5)


    Sequences (other than strings) are converted to json_build_array constructs

    ::

        json_sql([1, 2, 3])  # json_build_array(1, 2, 3)


    You can also nest these data structures

    ::

        json_sql({'a': [1, 2, 3]})
        # json_build_object('a', json_build_array[1, 2, 3])


    :param value:
        value to be converted to SQLAlchemy PostgreSQL function constructs
    """
    scalar_convert = sa.text
    if scalars_to_json:
        def scalar_convert(a):
            return sa.func.to_json(sa.text(a))

    if isinstance(value, Mapping):
        return sa.func.json_build_object(
            *(
                json_sql(v, scalars_to_json=False)
                for v in itertools.chain(*value.items())
            )
        )
    elif isinstance(value, str):
        return scalar_convert(f"'{value}'")
    elif isinstance(value, Sequence):
        return sa.func.json_build_array(
            *(
                json_sql(v, scalars_to_json=False)
                for v in value
            )
        )
    elif isinstance(value, (int, float)):
        return scalar_convert(str(value))
    return value


def jsonb_sql(value, scalars_to_jsonb=True):
    """
    Convert python data structures to PostgreSQL specific SQLAlchemy JSONB
    constructs. This function is extremly useful if you need to build
    PostgreSQL JSONB on python side.

    .. note::

        This function needs PostgreSQL >= 9.4

    Scalars are converted to to_jsonb SQLAlchemy function objects

    ::

        jsonb_sql(1)     # Equals SQL: to_jsonb(1)

        jsonb_sql('a')   # to_jsonb('a')


    Mappings are converted to jsonb_build_object constructs

    ::

        jsonb_sql({'a': 'c', '2': 5})  # jsonb_build_object('a', 'c', '2', 5)


    Sequences (other than strings) converted to jsonb_build_array constructs

    ::

        jsonb_sql([1, 2, 3])  # jsonb_build_array(1, 2, 3)


    You can also nest these data structures

    ::

        jsonb_sql({'a': [1, 2, 3]})
        # jsonb_build_object('a', jsonb_build_array[1, 2, 3])


    :param value:
        value to be converted to SQLAlchemy PostgreSQL function constructs
    :boolean jsonbb:
        Flag to alternatively convert the return with a to_jsonb construct
    """
    scalar_convert = sa.text
    if scalars_to_jsonb:
        def scalar_convert(a):
            return sa.func.to_jsonb(sa.text(a))

    if isinstance(value, Mapping):
        return sa.func.jsonb_build_object(
            *(
                jsonb_sql(v, scalars_to_jsonb=False)
                for v in itertools.chain(*value.items())
            )
        )
    elif isinstance(value, str):
        return scalar_convert(f"'{value}'")
    elif isinstance(value, Sequence):
        return sa.func.jsonb_build_array(
            *(
                jsonb_sql(v, scalars_to_jsonb=False)
                for v in value
            )
        )
    elif isinstance(value, (int, float)):
        return scalar_convert(str(value))
    return value


def has_index(column_or_constraint):
    """
    Return whether or not given column or the columns of given foreign key
    constraint have an index. A column has an index if it has a single column
    index or it is the first column in compound column index.

    A foreign key constraint has an index if the constraint columns are the
    first columns in compound column index.

    :param column_or_constraint:
        SQLAlchemy Column object or SA ForeignKeyConstraint object

    .. versionadded: 0.26.2

    .. versionchanged: 0.30.18
        Added support for foreign key constaints.

    ::

        from sqlalchemy_utils import has_index


        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            title = sa.Column(sa.String(100))
            is_published = sa.Column(sa.Boolean, index=True)
            is_deleted = sa.Column(sa.Boolean)
            is_archived = sa.Column(sa.Boolean)

            __table_args__ = (
                sa.Index('my_index', is_deleted, is_archived),
            )


        table = Article.__table__

        has_index(table.c.is_published) # True
        has_index(table.c.is_deleted)   # True
        has_index(table.c.is_archived)  # False


    Also supports primary key indexes

    ::

        from sqlalchemy_utils import has_index


        class ArticleTranslation(Base):
            __tablename__ = 'article_translation'
            id = sa.Column(sa.Integer, primary_key=True)
            locale = sa.Column(sa.String(10), primary_key=True)
            title = sa.Column(sa.String(100))


        table = ArticleTranslation.__table__

        has_index(table.c.locale)   # False
        has_index(table.c.id)       # True


    This function supports foreign key constraints as well

    ::


        class User(Base):
            __tablename__ = 'user'
            first_name = sa.Column(sa.Unicode(255), primary_key=True)
            last_name = sa.Column(sa.Unicode(255), primary_key=True)

        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            author_first_name = sa.Column(sa.Unicode(255))
            author_last_name = sa.Column(sa.Unicode(255))
            __table_args__ = (
                sa.ForeignKeyConstraint(
                    [author_first_name, author_last_name],
                    [User.first_name, User.last_name]
                ),
                sa.Index(
                    'my_index',
                    author_first_name,
                    author_last_name
                )
            )

        table = Article.__table__
        constraint = list(table.foreign_keys)[0].constraint

        has_index(constraint)  # True
    """
    table = column_or_constraint.table
    if not isinstance(table, sa.Table):
        raise TypeError(
            'Only columns belonging to Table objects are supported. Given '
            'column belongs to %r.' % table
        )
    primary_keys = table.primary_key.columns.values()
    if isinstance(column_or_constraint, sa.ForeignKeyConstraint):
        columns = list(column_or_constraint.columns.values())
    else:
        columns = [column_or_constraint]

    return (
        (primary_keys and starts_with(primary_keys, columns)) or
        any(
            starts_with(index.columns.values(), columns)
            for index in table.indexes
        )
    )


def has_unique_index(column_or_constraint):
    """
    Return whether or not given column or given foreign key constraint has a
    unique index.

    A column has a unique index if it has a single column primary key index or
    it has a single column UniqueConstraint.

    A foreign key constraint has a unique index if the columns of the
    constraint are the same as the columns of table primary key or the coluns
    of any unique index or any unique constraint of the given table.

    :param column: SQLAlchemy Column object

    .. versionadded: 0.27.1

    .. versionchanged: 0.30.18
        Added support for foreign key constaints.

        Fixed support for unique indexes (previously only worked for unique
        constraints)

    ::

        from sqlalchemy_utils import has_unique_index


        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            title = sa.Column(sa.String(100))
            is_published = sa.Column(sa.Boolean, unique=True)
            is_deleted = sa.Column(sa.Boolean)
            is_archived = sa.Column(sa.Boolean)


        table = Article.__table__

        has_unique_index(table.c.is_published) # True
        has_unique_index(table.c.is_deleted)   # False
        has_unique_index(table.c.id)           # True


    This function supports foreign key constraints as well

    ::


        class User(Base):
            __tablename__ = 'user'
            first_name = sa.Column(sa.Unicode(255), primary_key=True)
            last_name = sa.Column(sa.Unicode(255), primary_key=True)

        class Article(Base):
            __tablename__ = 'article'
            id = sa.Column(sa.Integer, primary_key=True)
            author_first_name = sa.Column(sa.Unicode(255))
            author_last_name = sa.Column(sa.Unicode(255))
            __table_args__ = (
                sa.ForeignKeyConstraint(
                    [author_first_name, author_last_name],
                    [User.first_name, User.last_name]
                ),
                sa.Index(
                    'my_index',
                    author_first_name,
                    author_last_name,
                    unique=True
                )
            )

        table = Article.__table__
        constraint = list(table.foreign_keys)[0].constraint

        has_unique_index(constraint)  # True


    :raises TypeError: if given column does not belong to a Table object
    """
    table = column_or_constraint.table
    if not isinstance(table, sa.Table):
        raise TypeError(
            'Only columns belonging to Table objects are supported. Given '
            'column belongs to %r.' % table
        )
    primary_keys = list(table.primary_key.columns.values())
    if isinstance(column_or_constraint, sa.ForeignKeyConstraint):
        columns = list(column_or_constraint.columns.values())
    else:
        columns = [column_or_constraint]

    return (
        (columns == primary_keys) or
        any(
            columns == list(constraint.columns.values())
            for constraint in table.constraints
            if isinstance(constraint, sa.sql.schema.UniqueConstraint)
        ) or
        any(
            columns == list(index.columns.values())
            for index in table.indexes
            if index.unique
        )
    )


def is_auto_assigned_date_column(column):
    """
    Returns whether or not given SQLAlchemy Column object's is auto assigned
    DateTime or Date.

    :param column: SQLAlchemy Column object
    """
    return (
        (
            isinstance(column.type, sa.DateTime) or
            isinstance(column.type, sa.Date)
        ) and
        (
            column.default or
            column.server_default or
            column.onupdate or
            column.server_onupdate
        )
    )


def _set_url_database(url: sa.engine.url.URL, database):
    """Set the database of an engine URL.

    :param url: A SQLAlchemy engine URL.
    :param database: New database to set.

    """
    if hasattr(url, '_replace'):
        # Cannot use URL.set() as database may need to be set to None.
        ret = url._replace(database=database)
    else:  # SQLAlchemy <1.4
        url = copy(url)
        url.database = database
        ret = url
    assert ret.database == database, ret
    return ret


def _get_scalar_result(engine, sql):
    with engine.connect() as conn:
        return conn.scalar(sql)


def _sqlite_file_exists(database):
    if not os.path.isfile(database) or os.path.getsize(database) < 100:
        return False

    with open(database, 'rb') as f:
        header = f.read(100)

    return header[:16] == b'SQLite format 3\x00'


def database_exists(url):
    """Check if a database exists.

    :param url: A SQLAlchemy engine URL.

    Performs backend-specific testing to quickly determine if a database
    exists on the server. ::

        database_exists('postgresql://postgres@localhost/name')  #=> False
        create_database('postgresql://postgres@localhost/name')
        database_exists('postgresql://postgres@localhost/name')  #=> True

    Supports checking against a constructed URL as well. ::

        engine = create_engine('postgresql://postgres@localhost/name')
        database_exists(engine.url)  #=> False
        create_database(engine.url)
        database_exists(engine.url)  #=> True

    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    engine = None
    try:
        if dialect_name == 'postgresql':
            text = "SELECT 1 FROM pg_database WHERE datname='%s'" % database
            for db in (database, 'postgres', 'template1', 'template0', None):
                url = _set_url_database(url, database=db)
                engine = sa.create_engine(url)
                try:
                    return bool(_get_scalar_result(engine, sa.text(text)))
                except (ProgrammingError, OperationalError):
                    pass
            return False

        elif dialect_name == 'mysql':
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            text = ("SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA "
                    "WHERE SCHEMA_NAME = '%s'" % database)
            return bool(_get_scalar_result(engine, sa.text(text)))

        elif dialect_name == 'sqlite':
            url = _set_url_database(url, database=None)
            engine = sa.create_engine(url)
            if database:
                return database == ':memory:' or _sqlite_file_exists(database)
            else:
                # The default SQLAlchemy database is in memory, and :memory: is
                # not required, thus we should support that use case.
                return True
        else:
            text = 'SELECT 1'
            try:
                engine = sa.create_engine(url)
                return bool(_get_scalar_result(engine, sa.text(text)))
            except (ProgrammingError, OperationalError):
                return False
    finally:
        if engine:
            engine.dispose()


def create_database(url, encoding='utf8', template=None):
    """Issue the appropriate CREATE DATABASE statement.

    :param url: A SQLAlchemy engine URL.
    :param encoding: The encoding to create the database as.
    :param template:
        The name of the template from which to create the new database. At the
        moment only supported by PostgreSQL driver.

    To create a database, you can pass a simple URL that would have
    been passed to ``create_engine``. ::

        create_database('postgresql://postgres@localhost/name')

    You may also pass the url from an existing engine. ::

        create_database(engine.url)

    Has full support for mysql, postgres, and sqlite. In theory,
    other database engines should be supported.
    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    dialect_driver = url.get_dialect().driver

    if dialect_name == 'postgresql':
        url = _set_url_database(url, database="postgres")
    elif dialect_name == 'mssql':
        url = _set_url_database(url, database="master")
    elif dialect_name == 'cockroachdb':
        url = _set_url_database(url, database="defaultdb")
    elif not dialect_name == 'sqlite':
        url = _set_url_database(url, database=None)

    if (dialect_name == 'mssql' and dialect_driver in {'pymssql', 'pyodbc'}) \
            or (dialect_name == 'postgresql' and dialect_driver in {
            'asyncpg', 'pg8000', 'psycopg', 'psycopg2', 'psycopg2cffi'}):
        engine = sa.create_engine(url, isolation_level='AUTOCOMMIT')
    else:
        engine = sa.create_engine(url)

    if dialect_name == 'postgresql':
        if not template:
            template = 'template1'

        with engine.begin() as conn:
            text = "CREATE DATABASE {} ENCODING '{}' TEMPLATE {}".format(
                quote(conn, database),
                encoding,
                quote(conn, template)
            )
            conn.execute(sa.text(text))

    elif dialect_name == 'mysql':
        with engine.begin() as conn:
            text = "CREATE DATABASE {} CHARACTER SET = '{}'".format(
                quote(conn, database),
                encoding
            )
            conn.execute(sa.text(text))

    elif dialect_name == 'sqlite' and database != ':memory:':
        if database:
            with engine.begin() as conn:
                conn.execute(sa.text('CREATE TABLE DB(id int)'))
                conn.execute(sa.text('DROP TABLE DB'))

    else:
        with engine.begin() as conn:
            text = f'CREATE DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))

    engine.dispose()


def drop_database(url):
    """Issue the appropriate DROP DATABASE statement.

    :param url: A SQLAlchemy engine URL.

    Works similar to the :ref:`create_database` method in that both url text
    and a constructed url are accepted. ::

        drop_database('postgresql://postgres@localhost/name')
        drop_database(engine.url)

    """

    url = make_url(url)
    database = url.database
    dialect_name = url.get_dialect().name
    dialect_driver = url.get_dialect().driver

    if dialect_name == 'postgresql':
        url = _set_url_database(url, database="postgres")
    elif dialect_name == 'mssql':
        url = _set_url_database(url, database="master")
    elif dialect_name == 'cockroachdb':
        url = _set_url_database(url, database="defaultdb")
    elif not dialect_name == 'sqlite':
        url = _set_url_database(url, database=None)

    if dialect_name == 'mssql' and dialect_driver in {'pymssql', 'pyodbc'}:
        engine = sa.create_engine(url, connect_args={'autocommit': True})
    elif dialect_name == 'postgresql' and dialect_driver in {
            'asyncpg', 'pg8000', 'psycopg', 'psycopg2', 'psycopg2cffi'}:
        engine = sa.create_engine(url, isolation_level='AUTOCOMMIT')
    else:
        engine = sa.create_engine(url)

    if dialect_name == 'sqlite' and database != ':memory:':
        if database:
            os.remove(database)
    elif dialect_name == 'postgresql':
        with engine.begin() as conn:
            # Disconnect all users from the database we are dropping.
            version = conn.dialect.server_version_info
            pid_column = (
                'pid' if (version >= (9, 2)) else 'procpid'
            )
            text = '''
            SELECT pg_terminate_backend(pg_stat_activity.{pid_column})
            FROM pg_stat_activity
            WHERE pg_stat_activity.datname = '{database}'
            AND {pid_column} <> pg_backend_pid();
            '''.format(pid_column=pid_column, database=database)
            conn.execute(sa.text(text))

            # Drop the database.
            text = f'DROP DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))
    else:
        with engine.begin() as conn:
            text = f'DROP DATABASE {quote(conn, database)}'
            conn.execute(sa.text(text))

    engine.dispose()
