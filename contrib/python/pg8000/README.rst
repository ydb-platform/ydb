======
pg8000
======

.. |ssl.SSLContext| replace:: ``ssl.SSLContext``
.. _ssl.SSLContext: https://docs.python.org/3/library/ssl.html#ssl.SSLContext

.. |ssl.create_default_context()| replace:: ``ssl.create_default_context()``
.. _ssl.create_default_context(): https://docs.python.org/3/library/ssl.html#ssl.create_default_context

pg8000 is a pure-`Python <https://www.python.org/>`_
`PostgreSQL <http://www.postgresql.org/>`_ driver that complies with
`DB-API 2.0 <http://www.python.org/dev/peps/pep-0249/>`_. It is tested on Python
versions 3.8+, on CPython and PyPy, and PostgreSQL versions 11+. pg8000's name comes
from the belief that it is probably about the 8000th PostgreSQL interface for Python.
pg8000 is distributed under the BSD 3-clause license.

All bug reports, feature requests and contributions are welcome at
`http://github.com/tlocke/pg8000/ <http://github.com/tlocke/pg8000/>`_.

.. image:: https://github.com/tlocke/pg8000/workflows/pg8000/badge.svg
   :alt: Build Status

.. contents:: Table of Contents
   :depth: 2
   :local:

Installation
------------

To install pg8000 using `pip` type:

`pip install pg8000`


Native API Interactive Examples
-------------------------------

pg8000 comes with two APIs, the native pg8000 API and the DB-API 2.0 standard
API. These are the examples for the native API, and the DB-API 2.0 examples
follow in the next section.


Basic Example
`````````````

Import pg8000, connect to the database, create a table, add some rows and then
query the table:

>>> import pg8000.native
>>>
>>> # Connect to the database with user name postgres
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> # Create a temporary table
>>>
>>> con.run("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")
>>>
>>> # Populate the table
>>>
>>> for title in ("Ender's Game", "The Magus"):
...     con.run("INSERT INTO book (title) VALUES (:title)", title=title)
>>>
>>> # Print all the rows in the table
>>>
>>> for row in con.run("SELECT * FROM book"):
...     print(row)
[1, "Ender's Game"]
[2, 'The Magus']
>>>
>>> con.close()


Transactions
````````````

Here's how to run groups of SQL statements in a
`transaction <https://www.postgresql.org/docs/current/tutorial-transactions.html>`_:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("START TRANSACTION")
>>>
>>> # Create a temporary table
>>> con.run("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")
>>>
>>> for title in ("Ender's Game", "The Magus", "Phineas Finn"):
...     con.run("INSERT INTO book (title) VALUES (:title)", title=title)
>>> con.run("COMMIT")
>>> for row in con.run("SELECT * FROM book"):
...     print(row)
[1, "Ender's Game"]
[2, 'The Magus']
[3, 'Phineas Finn']
>>>
>>> con.close()

rolling back a transaction:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> # Create a temporary table
>>> con.run("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")
>>>
>>> for title in ("Ender's Game", "The Magus", "Phineas Finn"):
...     con.run("INSERT INTO book (title) VALUES (:title)", title=title)
>>>
>>> con.run("START TRANSACTION")
>>> con.run("DELETE FROM book WHERE title = :title", title="Phineas Finn") 
>>> con.run("ROLLBACK")
>>> for row in con.run("SELECT * FROM book"):
...     print(row)
[1, "Ender's Game"]
[2, 'The Magus']
[3, 'Phineas Finn']
>>>
>>> con.close()

NB. There is `a longstanding bug <https://github.com/tlocke/pg8000/issues/36>`_
in the PostgreSQL server whereby if a `COMMIT` is issued against a failed
transaction, the transaction is silently rolled back, rather than an error being
returned. pg8000 attempts to detect when this has happened and raise an
`InterfaceError`.


Query Using Functions
`````````````````````

Another query, using some PostgreSQL functions:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT TO_CHAR(TIMESTAMP '2021-10-10', 'YYYY BC')")
[['2021 AD']]
>>>
>>> con.close()


Interval Type
`````````````

A query that returns the PostgreSQL interval type:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> import datetime
>>>
>>> ts = datetime.date(1980, 4, 27)
>>> con.run("SELECT timestamp '2013-12-01 16:06' - :ts", ts=ts)
[[datetime.timedelta(days=12271, seconds=57960)]]
>>>
>>> con.close()


Point Type
``````````

A round-trip with a
`PostgreSQL point <https://www.postgresql.org/docs/current/datatype-geometric.html>`_
type:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT CAST(:pt as point)", pt=(2.3,1))
[[(2.3, 1.0)]]
>>>
>>> con.close()


Client Encoding
```````````````

When communicating with the server, pg8000 uses the character set that the server asks
it to use (the client encoding). By default the client encoding is the database's
character set (chosen when the database is created), but the client encoding can be
changed in a number of ways (eg. setting ``CLIENT_ENCODING`` in ``postgresql.conf``).
Another way of changing the client encoding is by using an SQL command. For example:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SET CLIENT_ENCODING TO 'UTF8'")
>>> con.run("SHOW CLIENT_ENCODING")
[['UTF8']]
>>>
>>> con.close()


JSON
````

`JSON <https://www.postgresql.org/docs/current/datatype-json.html>`_ always comes back
from the server de-serialized. If the JSON you want to send is a ``dict`` then you can
just do:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> val = {'name': 'Apollo 11 Cave', 'zebra': True, 'age': 26.003}
>>> con.run("SELECT CAST(:apollo as jsonb)", apollo=val)
[[{'age': 26.003, 'name': 'Apollo 11 Cave', 'zebra': True}]]
>>>
>>> con.close()

JSON can always be sent in serialized form to the server:

>>> import json
>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>>
>>> val = ['Apollo 11 Cave', True, 26.003]
>>> con.run("SELECT CAST(:apollo as jsonb)", apollo=json.dumps(val))
[[['Apollo 11 Cave', True, 26.003]]]
>>>
>>> con.close()

JSON queries can be have parameters:

>>> import pg8000.native
>>>
>>> with pg8000.native.Connection("postgres", password="cpsnow") as con:
...     con.run(""" SELECT CAST('{"a":1, "b":2}' AS jsonb) @> :v """, v={"b": 2})
[[True]]


Retrieve Column Metadata From Results
`````````````````````````````````````

Find the column metadata returned from a query:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("create temporary table quark (id serial, name text)")
>>> for name in ('Up', 'Down'):
...     con.run("INSERT INTO quark (name) VALUES (:name)", name=name)
>>> # Now execute the query
>>>
>>> con.run("SELECT * FROM quark")
[[1, 'Up'], [2, 'Down']]
>>>
>>> # and retrieve the metadata
>>>
>>> con.columns
[{'table_oid': ..., 'column_attrnum': 1, 'type_oid': 23, 'type_size': 4, 'type_modifier': -1, 'format': 0, 'name': 'id'}, {'table_oid': ..., 'column_attrnum': 2, 'type_oid': 25, 'type_size': -1, 'type_modifier': -1, 'format': 0, 'name': 'name'}]
>>>
>>> # Show just the column names
>>>
>>> [c['name'] for c in con.columns]
['id', 'name']
>>>
>>> con.close()


Notices And Notifications
`````````````````````````

PostgreSQL `notices
<https://www.postgresql.org/docs/current/static/plpgsql-errors-and-messages.html>`_ are
stored in a deque called ``Connection.notices`` and added using the ``append()``
method. Similarly there are ``Connection.notifications`` for `notifications
<https://www.postgresql.org/docs/current/static/sql-notify.html>`_ and
``Connection.parameter_statuses`` for changes to the server configuration. Here's an
example:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("LISTEN aliens_landed")
>>> con.run("NOTIFY aliens_landed")
>>> # A notification is a tuple containing (backend_pid, channel, payload)
>>>
>>> con.notifications[0]
(..., 'aliens_landed', '')
>>>
>>> con.close()


LIMIT ALL
`````````

You might think that the following would work, but in fact it fails:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT 'silo 1' LIMIT :lim", lim='ALL')
Traceback (most recent call last):
pg8000.exceptions.DatabaseError: ...
>>>
>>> con.close()

Instead the `docs say <https://www.postgresql.org/docs/current/sql-select.html>`_ that
you can send ``null`` as an alternative to ``ALL``, which does work:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT 'silo 1' LIMIT :lim", lim=None)
[['silo 1']]
>>>
>>> con.close()


IN and NOT IN
`````````````

You might think that the following would work, but in fact the server doesn't like it:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT 'silo 1' WHERE 'a' IN :v", v=['a', 'b'])
Traceback (most recent call last):
pg8000.exceptions.DatabaseError: ...
>>>
>>> con.close()

instead you can write it using the `unnest
<https://www.postgresql.org/docs/current/functions-array.html>`_ function:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run(
...     "SELECT 'silo 1' WHERE 'a' IN (SELECT unnest(CAST(:v as varchar[])))",
...     v=['a', 'b'])
[['silo 1']]
>>> con.close()

and you can do the same for ``NOT IN``.


Many SQL Statements Can't Be Parameterized
``````````````````````````````````````````

In PostgreSQL parameters can only be used for `data values, not identifiers
<https://www.postgresql.org/docs/current/xfunc-sql.html#XFUNC-SQL-FUNCTION-ARGUMENTS>`_.
Sometimes this might not work as expected, for example the following fails:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> channel = 'top_secret'
>>>
>>> con.run("LISTEN :channel", channel=channel)
Traceback (most recent call last):
pg8000.exceptions.DatabaseError: ...
>>>
>>> con.close()

It fails because the PostgreSQL server doesn't allow this statement to have any
parameters. There are many SQL statements that one might think would have parameters,
but don't. For these cases the SQL has to be created manually, being careful to use the
``identifier()`` and ``literal()`` functions to escape the values to avoid `SQL
injection attacks <https://en.wikipedia.org/wiki/SQL_injection>`_:

>>> from pg8000.native import Connection, identifier, literal
>>>
>>> con = Connection("postgres", password="cpsnow")
>>>
>>> channel = 'top_secret'
>>> payload = 'Aliens Landed!'
>>> con.run(f"LISTEN {identifier(channel)}")
>>> con.run(f"NOTIFY {identifier(channel)}, {literal(payload)}")
>>>
>>> con.notifications[0]
(..., 'top_secret', 'Aliens Landed!')
>>>
>>> con.close()


COPY FROM And TO A Stream
`````````````````````````

The SQL `COPY <https://www.postgresql.org/docs/current/sql-copy.html>`_ statement can be
used to copy from and to a file or file-like object. Here' an example using the CSV
format:

>>> import pg8000.native
>>> from io import StringIO
>>> import csv
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> # Create a CSV file in memory
>>>
>>> stream_in = StringIO()
>>> csv_writer = csv.writer(stream_in)
>>> csv_writer.writerow([1, "electron"])
12
>>> csv_writer.writerow([2, "muon"])
8
>>> csv_writer.writerow([3, "tau"])
7
>>> stream_in.seek(0)
0
>>>
>>> # Create a table and then copy the CSV into it
>>>
>>> con.run("CREATE TEMPORARY TABLE lepton (id SERIAL, name TEXT)")
>>> con.run("COPY lepton FROM STDIN WITH (FORMAT CSV)", stream=stream_in)
>>>
>>> # COPY from a table to a stream
>>>
>>> stream_out = StringIO()
>>> con.run("COPY lepton TO STDOUT WITH (FORMAT CSV)", stream=stream_out)
>>> stream_out.seek(0)
0
>>> for row in csv.reader(stream_out):
...     print(row)
['1', 'electron']
['2', 'muon']
['3', 'tau']
>>>
>>> con.close()

It's also possible to COPY FROM an iterable, which is useful if you're creating rows
programmatically:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> # Generator function for creating rows
>>> def row_gen():
...     for i, name in ((1, "electron"), (2, "muon"), (3, "tau")):
...         yield f"{i},{name}\n"
>>>
>>> # Create a table and then copy the CSV into it
>>>
>>> con.run("CREATE TEMPORARY TABLE lepton (id SERIAL, name TEXT)")
>>> con.run("COPY lepton FROM STDIN WITH (FORMAT CSV)", stream=row_gen())
>>>
>>> # COPY from a table to a stream
>>>
>>> stream_out = StringIO()
>>> con.run("COPY lepton TO STDOUT WITH (FORMAT CSV)", stream=stream_out)
>>> stream_out.seek(0)
0
>>> for row in csv.reader(stream_out):
...     print(row)
['1', 'electron']
['2', 'muon']
['3', 'tau']
>>>
>>> con.close()


Execute Multiple SQL Statements
```````````````````````````````

If you want to execute a series of SQL statements (eg. an ``.sql`` file), you can run
them as expected:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> statements = "SELECT 5; SELECT 'Erich Fromm';"
>>>
>>> con.run(statements)
[[5], ['Erich Fromm']]
>>>
>>> con.close()

The only caveat is that when executing multiple statements you can't have any
parameters.


Quoted Identifiers in SQL
`````````````````````````

Say you had a column called ``My Column``. Since it's case sensitive and contains a
space, you'd have to `surround it by double quotes
<https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIER>`_.
But you can't do:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("select 'hello' as "My Column"")
Traceback (most recent call last):
SyntaxError: invalid syntax...
>>>
>>> con.close()

since Python uses double quotes to delimit string literals, so one solution is
to use Python's `triple quotes
<https://docs.python.org/3/tutorial/introduction.html#strings>`_ to delimit the string
instead:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run('''SELECT 'hello' AS "My Column"''')
[['hello']]
>>>
>>> con.close()

another solution, that's especially useful if the identifier comes from an untrusted
source, is to use the ``identifier()`` function, which correctly quotes and escapes the
identifier as needed:

>>> from pg8000.native import Connection, identifier
>>>
>>> con = Connection("postgres", password="cpsnow")
>>>
>>> sql = f"SELECT 'hello' as {identifier('My Column')}"
>>> print(sql)
SELECT 'hello' as "My Column"
>>>
>>> con.run(sql)
[['hello']]
>>>
>>> con.close()

this approach guards against `SQL injection attacks
<https://en.wikipedia.org/wiki/SQL_injection>`_. One thing to note if you're using
explicit schemas (eg. ``pg_catalog.pg_language``) is that the schema name and table name
are both separate identifiers. So to escape them you'd do:

>>> from pg8000.native import Connection, identifier
>>>
>>> con = Connection("postgres", password="cpsnow")
>>>
>>> query = (
...     f"SELECT lanname FROM {identifier('pg_catalog')}.{identifier('pg_language')} "
...     f"WHERE lanname = 'sql'"
... )
>>> print(query)
SELECT lanname FROM pg_catalog.pg_language WHERE lanname = 'sql'
>>>
>>> con.run(query)
[['sql']]
>>>
>>> con.close()


Custom adapter from a Python type to a PostgreSQL type
``````````````````````````````````````````````````````

pg8000 has a mapping from Python types to PostgreSQL types for when it needs to send
SQL parameters to the server. The default mapping that comes with pg8000 is designed to
work well in most cases, but you might want to add or replace the default mapping.

A Python ``datetime.timedelta`` object is sent to the server as a PostgreSQL
``interval`` type,  which has the ``oid`` 1186. But let's say we wanted to create our
own Python class to be sent as an ``interval`` type. Then we'd have to register an
adapter:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> class MyInterval(str):
...     pass
>>>
>>> def my_interval_out(my_interval):
...     return my_interval  # Must return a str
>>>
>>> con.register_out_adapter(MyInterval, my_interval_out)
>>> con.run("SELECT CAST(:interval as interval)", interval=MyInterval("2 hours"))
[[datetime.timedelta(seconds=7200)]]
>>>
>>> con.close()

Note that it still came back as a ``datetime.timedelta`` object because we only changed
the mapping from Python to PostgreSQL. See below for an example of how to change the
mapping from PostgreSQL to Python.


Custom adapter from a PostgreSQL type to a Python type
``````````````````````````````````````````````````````

pg8000 has a mapping from PostgreSQL types to Python types for when it receives SQL
results from the server. The default mapping that comes with pg8000 is designed to work
well in most cases, but you might want to add or replace the default mapping.

If pg8000 receives PostgreSQL ``interval`` type, which has the ``oid`` 1186, it converts
it into a Python ``datetime.timedelta`` object. But let's say we wanted to create our
own Python class to be used instead of ``datetime.timedelta``. Then we'd have to
register an adapter:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> class MyInterval(str):
...     pass
>>>
>>> def my_interval_in(my_interval_str):  # The parameter is of type str
...     return MyInterval(my_interval)
>>>
>>> con.register_in_adapter(1186, my_interval_in)
>>> con.run("SELECT \'2 years'")
[['2 years']]
>>>
>>> con.close()

Note that registering the 'in' adapter only afects the mapping from the PostgreSQL type
to the Python type. See above for an example of how to change the mapping from
PostgreSQL to Python.


Could Not Determine Data Type Of Parameter
``````````````````````````````````````````

Sometimes you'll get the 'could not determine data type of parameter' error message from
the server:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT :v IS NULL", v=None)
Traceback (most recent call last):
pg8000.exceptions.DatabaseError: {'S': 'ERROR', 'V': 'ERROR', 'C': '42P18', 'M': 'could not determine data type of parameter $1', 'F': 'postgres.c', 'L': '...', 'R': 'exec_parse_message'}
>>>
>>> con.close()

One way of solving it is to put a ``CAST`` in the SQL:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT cast(:v as TIMESTAMP) IS NULL", v=None)
[[True]]
>>>
>>> con.close()

Another way is to override the type that pg8000 sends along with each parameter:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> con.run("SELECT :v IS NULL", v=None, types={'v': pg8000.native.TIMESTAMP})
[[True]]
>>>
>>> con.close()


Prepared Statements
```````````````````

`Prepared statements <https://www.postgresql.org/docs/current/sql-prepare.html>`_
can be useful in improving performance when you have a statement that's executed
repeatedly. Here's an example:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection("postgres", password="cpsnow")
>>>
>>> # Create the prepared statement
>>> ps = con.prepare("SELECT cast(:v as varchar)")
>>>
>>> # Execute the statement repeatedly
>>> ps.run(v="speedy")
[['speedy']]
>>> ps.run(v="rapid")
[['rapid']]
>>> ps.run(v="swift")
[['swift']]
>>>
>>> # Close the prepared statement, releasing resources on the server
>>> ps.close()
>>>
>>> con.close()


Use Environment Variables As Connection Defaults
````````````````````````````````````````````````

You might want to use the current user as the database username for example:

>>> import pg8000.native
>>> import getpass
>>>
>>> # Connect to the database with current user name
>>> username = getpass.getuser()
>>> connection = pg8000.native.Connection(username, password="cpsnow")
>>>
>>> connection.run("SELECT 'pilau'")
[['pilau']]
>>>
>>> connection.close()

or perhaps you may want to use some of the same `environment variables that libpg uses
<https://www.postgresql.org/docs/current/libpq-envars.html>`_:

>>> import pg8000.native
>>> from os import environ
>>>
>>> username = environ.get('PGUSER', 'postgres')
>>> password = environ.get('PGPASSWORD', 'cpsnow')
>>> host = environ.get('PGHOST', 'localhost')
>>> port = environ.get('PGPORT', '5432')
>>> database = environ.get('PGDATABASE')
>>>
>>> connection = pg8000.native.Connection(
...     username, password=password, host=host, port=port, database=database)
>>>
>>> connection.run("SELECT 'Mr Cairo'")
[['Mr Cairo']]
>>>
>>> connection.close()

It might be asked, why doesn't pg8000 have this behaviour built in? The thinking
follows the second aphorism of `The Zen of Python
<https://www.python.org/dev/peps/pep-0020/>`_:

    Explicit is better than implicit.

So we've taken the approach of only being able to set connection parameters using the
``pg8000.native.Connection()`` constructor.


Connect To PostgreSQL Over SSL
``````````````````````````````

To connect to the server using SSL defaults do::

  import pg8000.native
  connection = pg8000.native.Connection('postgres', password="cpsnow", ssl_context=True)
  connection.run("SELECT 'The game is afoot!'")

To connect over SSL with custom settings, set the ``ssl_context`` parameter to an
|ssl.SSLContext|_ object:

::

  import pg8000.native
  import ssl


  ssl_context = ssl.create_default_context()
  ssl_context.verify_mode = ssl.CERT_REQUIRED
  ssl_context.load_verify_locations('root.pem')        
  connection = pg8000.native.Connection(
    'postgres', password="cpsnow", ssl_context=ssl_context)

It may be that your PostgreSQL server is behind an SSL proxy server in which case you
can set a pg8000-specific attribute ``ssl.SSLContext.request_ssl = False`` which tells
pg8000 to connect using an SSL socket, but not to request SSL from the PostgreSQL
server:

::

  import pg8000.native
  import ssl

  ssl_context = ssl.create_default_context()
  ssl_context.request_ssl = False
  connection = pg8000.native.Connection(
      'postgres', password="cpsnow", ssl_context=ssl_context)


Server-Side Cursors
```````````````````

You can use the SQL commands `DECLARE
<https://www.postgresql.org/docs/current/sql-declare.html>`_,
`FETCH <https://www.postgresql.org/docs/current/sql-fetch.html>`_,
`MOVE <https://www.postgresql.org/docs/current/sql-move.html>`_ and
`CLOSE <https://www.postgresql.org/docs/current/sql-close.html>`_ to manipulate
server-side cursors. For example:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection('postgres', password="cpsnow")
>>> con.run("START TRANSACTION")
>>> con.run("DECLARE c SCROLL CURSOR FOR SELECT * FROM generate_series(1, 100)")
>>> con.run("FETCH FORWARD 5 FROM c")
[[1], [2], [3], [4], [5]]
>>> con.run("MOVE FORWARD 50 FROM c")
>>> con.run("FETCH BACKWARD 10 FROM c")
[[54], [53], [52], [51], [50], [49], [48], [47], [46], [45]]
>>> con.run("CLOSE c")
>>> con.run("ROLLBACK")
>>>
>>> con.close()


BLOBs (Binary Large Objects)
````````````````````````````

There's a set of `SQL functions
<https://www.postgresql.org/docs/current/lo-funcs.html>`_ for manipulating BLOBs.
Here's an example:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection('postgres', password="cpsnow")
>>>
>>> # Create a BLOB and get its oid
>>> data = b'hello'
>>> res = con.run("SELECT lo_from_bytea(0, :data)", data=data)
>>> oid = res[0][0]
>>>
>>> # Create a table and store the oid of the BLOB
>>> con.run("CREATE TEMPORARY TABLE image (raster oid)")
>>>
>>> con.run("INSERT INTO image (raster) VALUES (:oid)", oid=oid)
>>> # Retrieve the data using the oid
>>> con.run("SELECT lo_get(:oid)", oid=oid)
[[b'hello']]
>>>
>>> # Add some data to the end of the BLOB
>>> more_data = b' all'
>>> offset = len(data)
>>> con.run(
...     "SELECT lo_put(:oid, :offset, :data)",
...     oid=oid, offset=offset, data=more_data)
[['']]
>>> con.run("SELECT lo_get(:oid)", oid=oid)
[[b'hello all']]
>>>
>>> # Download a part of the data
>>> con.run("SELECT lo_get(:oid, 6, 3)", oid=oid)
[[b'all']]
>>>
>>> con.close()


Replication Protocol
````````````````````

The PostgreSQL `Replication Protocol
<https://www.postgresql.org/docs/current/protocol-replication.html>`_ is supported using
the ``replication`` keyword when creating a connection:

>>> import pg8000.native
>>>
>>> con = pg8000.native.Connection(
...    'postgres', password="cpsnow", replication="database")
>>>
>>> con.run("IDENTIFY_SYSTEM")
[['...', 1, '0/...', 'postgres']]
>>>
>>> con.close()


DB-API 2 Interactive Examples
-----------------------------

These examples stick to the DB-API 2.0 standard.


Basic Example
`````````````

Import pg8000, connect to the database, create a table, add some rows and then query the
table:

>>> import pg8000.dbapi
>>>
>>> conn = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cursor = conn.cursor()
>>> cursor.execute("CREATE TEMPORARY TABLE book (id SERIAL, title TEXT)")
>>> cursor.execute(
...     "INSERT INTO book (title) VALUES (%s), (%s) RETURNING id, title",
...     ("Ender's Game", "Speaker for the Dead"))
>>> results = cursor.fetchall()
>>> for row in results:
...     id, title = row
...     print("id = %s, title = %s" % (id, title))
id = 1, title = Ender's Game
id = 2, title = Speaker for the Dead
>>> conn.commit()
>>>
>>> conn.close()


Query Using Functions
`````````````````````

Another query, using some PostgreSQL functions:

>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cursor = con.cursor()
>>>
>>> cursor.execute("SELECT TO_CHAR(TIMESTAMP '2021-10-10', 'YYYY BC')")
>>> cursor.fetchone()
['2021 AD']
>>>
>>> con.close()


Interval Type
`````````````

A query that returns the PostgreSQL interval type:

>>> import datetime
>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cursor = con.cursor()
>>>
>>> cursor.execute("SELECT timestamp '2013-12-01 16:06' - %s",
... (datetime.date(1980, 4, 27),))
>>> cursor.fetchone()
[datetime.timedelta(days=12271, seconds=57960)]
>>>
>>> con.close()


Point Type
``````````

A round-trip with a `PostgreSQL point
<https://www.postgresql.org/docs/current/datatype-geometric.html>`_ type:

>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cursor = con.cursor()
>>>
>>> cursor.execute("SELECT cast(%s as point)", ((2.3,1),))
>>> cursor.fetchone()
[(2.3, 1.0)]
>>>
>>> con.close()


Numeric Parameter Style
```````````````````````

pg8000 supports all the DB-API parameter styles. Here's an example of using the
'numeric' parameter style:

>>> import pg8000.dbapi
>>>
>>> pg8000.dbapi.paramstyle = "numeric"
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cursor = con.cursor()
>>>
>>> cursor.execute("SELECT array_prepend(:1, CAST(:2 AS int[]))", (500, [1, 2, 3, 4],))
>>> cursor.fetchone()
[[500, 1, 2, 3, 4]]
>>> pg8000.dbapi.paramstyle = "format"
>>>
>>> con.close()


Autocommit
``````````

Following the DB-API specification, autocommit is off by default. It can be turned on by
using the autocommit property of the connection:

>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> con.autocommit = True
>>>
>>> cur = con.cursor()
>>> cur.execute("vacuum")
>>> conn.autocommit = False
>>> cur.close()
>>>
>>> con.close()


Client Encoding
```````````````

When communicating with the server, pg8000 uses the character set that the server asks
it to use (the client encoding). By default the client encoding is the database's
character set (chosen when the database is created), but the client encoding can be
changed in a number of ways (eg. setting ``CLIENT_ENCODING`` in ``postgresql.conf``).
Another way of changing the client encoding is by using an SQL command. For example:

>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cur = con.cursor()
>>> cur.execute("SET CLIENT_ENCODING TO 'UTF8'")
>>> cur.execute("SHOW CLIENT_ENCODING")
>>> cur.fetchone()
['UTF8']
>>> cur.close()
>>>
>>> con.close()


JSON
````

JSON is sent to the server serialized, and returned de-serialized. Here's an example:

>>> import json
>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cur = con.cursor()
>>> val = ['Apollo 11 Cave', True, 26.003]
>>> cur.execute("SELECT cast(%s as json)", (json.dumps(val),))
>>> cur.fetchone()
[['Apollo 11 Cave', True, 26.003]]
>>> cur.close()
>>>
>>> con.close()

JSON queries can be have parameters:

>>> import pg8000.dbapi
>>>
>>> with pg8000.dbapi.connect("postgres", password="cpsnow") as con:
...     cur = con.cursor()
...     cur.execute(""" SELECT CAST('{"a":1, "b":2}' AS jsonb) @> %s """, ({"b": 2},))
...     for row in cur.fetchall():
...         print(row)
[True]


Retrieve Column Names From Results
``````````````````````````````````

Use the columns names retrieved from a query:

>>> import pg8000
>>> conn = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> c = conn.cursor()
>>> c.execute("create temporary table quark (id serial, name text)")
>>> c.executemany("INSERT INTO quark (name) VALUES (%s)", (("Up",), ("Down",)))
>>> #
>>> # Now retrieve the results
>>> #
>>> c.execute("select * from quark")
>>> rows = c.fetchall()
>>> keys = [k[0] for k in c.description]
>>> results = [dict(zip(keys, row)) for row in rows]
>>> assert results == [{'id': 1, 'name': 'Up'}, {'id': 2, 'name': 'Down'}]
>>>
>>> conn.close()


COPY from and to a file
```````````````````````

The SQL `COPY <https://www.postgresql.org/docs/current/sql-copy.html>`__ statement can
be used to copy from and to a file or file-like object:

>>> from io import StringIO
>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cur = con.cursor()
>>> #
>>> # COPY from a stream to a table
>>> #
>>> stream_in = StringIO('1\telectron\n2\tmuon\n3\ttau\n')
>>> cur = con.cursor()
>>> cur.execute("create temporary table lepton (id serial, name text)")
>>> cur.execute("COPY lepton FROM stdin", stream=stream_in)
>>> #
>>> # Now COPY from a table to a stream
>>> #
>>> stream_out = StringIO()
>>> cur.execute("copy lepton to stdout", stream=stream_out)
>>> stream_out.getvalue()
'1\telectron\n2\tmuon\n3\ttau\n'
>>>
>>> con.close()


Server-Side Cursors
```````````````````

You can use the SQL commands `DECLARE
<https://www.postgresql.org/docs/current/sql-declare.html>`_,
`FETCH <https://www.postgresql.org/docs/current/sql-fetch.html>`_,
`MOVE <https://www.postgresql.org/docs/current/sql-move.html>`_ and
`CLOSE <https://www.postgresql.org/docs/current/sql-close.html>`_ to manipulate
server-side cursors. For example:

>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cur = con.cursor()
>>> cur.execute("START TRANSACTION")
>>> cur.execute(
...    "DECLARE c SCROLL CURSOR FOR SELECT * FROM generate_series(1, 100)")
>>> cur.execute("FETCH FORWARD 5 FROM c")
>>> cur.fetchall()
([1], [2], [3], [4], [5])
>>> cur.execute("MOVE FORWARD 50 FROM c")
>>> cur.execute("FETCH BACKWARD 10 FROM c")
>>> cur.fetchall()
([54], [53], [52], [51], [50], [49], [48], [47], [46], [45])
>>> cur.execute("CLOSE c")
>>> cur.execute("ROLLBACK")
>>>
>>> con.close()


BLOBs (Binary Large Objects)
````````````````````````````

There's a set of `SQL functions
<https://www.postgresql.org/docs/current/lo-funcs.html>`_ for manipulating BLOBs.
Here's an example:

>>> import pg8000.dbapi
>>>
>>> con = pg8000.dbapi.connect(user="postgres", password="cpsnow")
>>> cur = con.cursor()
>>>
>>> # Create a BLOB and get its oid
>>> data = b'hello'
>>> cur = con.cursor()
>>> cur.execute("SELECT lo_from_bytea(0, %s)", [data])
>>> oid = cur.fetchone()[0]
>>>
>>> # Create a table and store the oid of the BLOB
>>> cur.execute("CREATE TEMPORARY TABLE image (raster oid)")
>>> cur.execute("INSERT INTO image (raster) VALUES (%s)", [oid])
>>>
>>> # Retrieve the data using the oid
>>> cur.execute("SELECT lo_get(%s)", [oid])
>>> cur.fetchall()
([b'hello'],)
>>>
>>> # Add some data to the end of the BLOB
>>> more_data = b' all'
>>> offset = len(data)
>>> cur.execute("SELECT lo_put(%s, %s, %s)", [oid, offset, more_data])
>>> cur.execute("SELECT lo_get(%s)", [oid])
>>> cur.fetchall()
([b'hello all'],)
>>>
>>> # Download a part of the data
>>> cur.execute("SELECT lo_get(%s, 6, 3)", [oid])
>>> cur.fetchall()
([b'all'],)
>>>
>>> con.close()


Type Mapping
------------

The following table shows the default mapping between Python types and PostgreSQL types,
and vice versa.

If pg8000 doesn't recognize a type that it receives from PostgreSQL, it will return it
as a ``str`` type. This is how pg8000 handles PostgreSQL ``enum`` and XML types. It's
possible to change the default mapping using adapters (see the examples).

.. table:: Python to PostgreSQL Type Mapping

   +-----------------------+-----------------+-----------------------------------------+
   | Python Type           | PostgreSQL Type | Notes                                   |
   +=======================+=================+=========================================+
   | bool                  | bool            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | int                   | int4            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | str                   | text            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | float                 | float8          |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | decimal.Decimal       | numeric         |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | bytes                 | bytea           |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | datetime.datetime     | timestamp       | +/-infinity PostgreSQL values are       |
   | (without tzinfo)      | without         | represented as Python ``str`` values.   |
   |                       | timezone        | If a ``timestamp`` is too big for       |
   |                       |                 | ``datetime.datetime`` then a ``str`` is |
   |                       |                 | used.                                   |
   +-----------------------+-----------------+-----------------------------------------+
   | datetime.datetime     | timestamp with  | +/-infinity PostgreSQL values are       |
   | (with tzinfo)         | timezone        | represented as Python ``str`` values.   |
   |                       |                 | If a ``timestamptz`` is too big for     |
   |                       |                 | ``datetime.datetime`` then a ``str`` is |
   |                       |                 | used.                                   |
   +-----------------------+-----------------+-----------------------------------------+
   | datetime.date         | date            | +/-infinity PostgreSQL values are       |
   |                       |                 | represented as Python ``str`` values.   |
   |                       |                 | If a ``date`` is too big for a          |
   |                       |                 | ``datetime.date`` then a ``str`` is     |
   |                       |                 | used.                                   |
   +-----------------------+-----------------+-----------------------------------------+
   | datetime.time         | time without    |                                         |
   |                       | time zone       |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | datetime.timedelta    | interval        | If an ``interval`` is too big for       |
   |                       |                 | ``datetime.timedelta`` then a           |
   |                       |                 | ``PGInterval``  is used.                |
   +-----------------------+-----------------+-----------------------------------------+
   | None                  | NULL            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | uuid.UUID             | uuid            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | ipaddress.IPv4Address | inet            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | ipaddress.IPv6Address | inet            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | ipaddress.IPv4Network | inet            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | ipaddress.IPv6Network | inet            |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | int                   | xid             |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | list of int           | INT4[]          |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | list of float         | FLOAT8[]        |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | list of bool          | BOOL[]          |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | list of str           | TEXT[]          |                                         |
   +-----------------------+-----------------+-----------------------------------------+
   | int                   | int2vector      | Only from PostgreSQL to Python          |
   +-----------------------+-----------------+-----------------------------------------+
   | JSON                  | json, jsonb     | The Python JSON is provided as a Python |
   |                       |                 | serialized string. Results returned as  |
   |                       |                 | de-serialized JSON.                     |
   +-----------------------+-----------------+-----------------------------------------+
   | pg8000.Range          | \*range         | PostgreSQL multirange types are         |
   |                       |                 | represented in Python as a list of      |
   |                       |                 | range types.                            |
   +-----------------------+-----------------+-----------------------------------------+
   | tuple                 | composite type  | Only from Python to PostgreSQL          |
   +-----------------------+-----------------+-----------------------------------------+



Theory Of Operation
-------------------

  A concept is tolerated inside the microkernel only if moving it outside the kernel,
  i.e., permitting competing implementations, would prevent the implementation of the
  system's required functionality.

  -- Jochen Liedtke, Liedtke's minimality principle

pg8000 is designed to be used with one thread per connection.

Pg8000 communicates with the database using the `PostgreSQL Frontend/Backend Protocol
<https://www.postgresql.org/docs/current/protocol.html>`_ (FEBE). If a query has no
parameters, pg8000 uses the 'simple query protocol'. If a query does have parameters,
pg8000 uses the 'extended query protocol' with unnamed prepared statements. The steps
for a query with parameters are:

1. Query comes in.

#. Send a PARSE message to the server to create an unnamed prepared statement.

#. Send a BIND message to run against the unnamed prepared statement, resulting in an
   unnamed portal on the server.

#. Send an EXECUTE message to read all the results from the portal.

It's also possible to use named prepared statements. In which case the prepared
statement persists on the server, and represented in pg8000 using a
``PreparedStatement`` object. This means that the PARSE step gets executed once up
front, and then only the BIND and EXECUTE steps are repeated subsequently.

There are a lot of PostgreSQL data types, but few primitive data types in Python. By
default, pg8000 doesn't send PostgreSQL data type information in the PARSE step, in
which case PostgreSQL assumes the types implied by the SQL statement. In some cases
PostgreSQL can't work out a parameter type and so an `explicit cast
<https://www.postgresql.org/docs/current/static/sql-expressions.html#SQL-SYNTAX-TYPE-CASTS>`_
can be used in the SQL.

In the FEBE protocol, each query parameter can be sent to the server either as binary
or text according to the format code. In pg8000 the parameters are always sent as text.

Occasionally, the network connection between pg8000 and the server may go down. If
pg8000 encounters a network problem it'll raise an ``InterfaceError`` with the message
``network error`` and with the original exception set as the `cause
<https://docs.python.org/3/reference/simple_stmts.html#the-raise-statement>`_.


Native API Docs
---------------

pg8000.native.Error
```````````````````

Generic exception that is the base exception of the other error exceptions.


pg8000.native.InterfaceError
````````````````````````````

For errors that originate within pg8000.


pg8000.native.DatabaseError
```````````````````````````

For errors that originate from the server.

pg8000.native.Connection(user, host='localhost', database=None, port=5432, password=None, source_address=None, unix_sock=None, ssl_context=None, timeout=None, tcp_keepalive=True, application_name=None, replication=None, sock=None)
``````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````````

Creates a connection to a PostgreSQL database.

user
  The username to connect to the PostgreSQL server with. If your server character
  encoding is not ``ascii`` or ``utf8``, then you need to provide ``user`` as bytes,
  eg. ``'my_name'.encode('EUC-JP')``.

host
  The hostname of the PostgreSQL server to connect with. Providing this parameter is
  necessary for TCP/IP connections. One of either ``host`` or ``unix_sock`` must be
  provided. The default is ``localhost``.

database
  The name of the database instance to connect with. If ``None`` then the PostgreSQL
  server will assume the database name is the same as the username. If your server
  character encoding is not ``ascii`` or ``utf8``, then you need to provide ``database``
  as bytes, eg. ``'my_db'.encode('EUC-JP')``.

port
  The TCP/IP port of the PostgreSQL server instance.  This parameter defaults to
  ``5432``, the registered common port of PostgreSQL TCP/IP servers.

password
  The user password to connect to the server with. This parameter is optional; if
  omitted and the database server requests password-based authentication, the connection
  will fail to open. If this parameter is provided but not
  requested by the server, no error will occur.

  If your server character encoding is not ``ascii`` or ``utf8``, then you need to
  provide ``password`` as bytes, eg.  ``'my_password'.encode('EUC-JP')``.

source_address
  The source IP address which initiates the connection to the PostgreSQL server. The
  default is ``None`` which means that the operating system will choose the source
  address.

unix_sock
  The path to the UNIX socket to access the database through, for example,
  ``'/tmp/.s.PGSQL.5432'``. One of either ``host`` or ``unix_sock`` must be provided.

ssl_context
  This governs SSL encryption for TCP/IP sockets. It can have three values:

  - ``None``, meaning no SSL (the default)

  - ``True``, means use SSL with an |ssl.SSLContext|_ created using
    |ssl.create_default_context()|_

  - An instance of |ssl.SSLContext|_ which will be used to create the SSL connection.

  If your PostgreSQL server is behind an SSL proxy, you can set the pg8000-specific
  attribute ``ssl.SSLContext.request_ssl = False``, which tells pg8000 to use an SSL
  socket, but not to request SSL from the PostgreSQL server. Note that this means you
  can't use SCRAM authentication with channel binding.

timeout
  This is the time in seconds before the connection to the server will time out. The
  default is ``None`` which means no timeout.

tcp_keepalive
  If ``True`` then use `TCP keepalive
  <https://en.wikipedia.org/wiki/Keepalive#TCP_keepalive>`_. The default is ``True``.

application_name
  Sets the `application_name
  <https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME>`_.
  If your server character encoding is not ``ascii`` or ``utf8``, then you need to
  provide values as bytes, eg.  ``'my_application_name'.encode('EUC-JP')``. The default
  is ``None`` which means that the server will set the application name.

replication
  Used to run in `streaming replication mode
  <https://www.postgresql.org/docs/current/protocol-replication.html>`_. If your server
  character encoding is not ``ascii`` or ``utf8``, then you need to provide values as
  bytes, eg. ``'database'.encode('EUC-JP')``.

sock
  A socket-like object to use for the connection. For example, ``sock`` could be a plain
  ``socket.socket``, or it could represent an SSH tunnel or perhaps an
  ``ssl.SSLSocket`` to an SSL proxy. If an |ssl.SSLContext| is provided, then it will be
  used to attempt to create an SSL socket from the provided socket. 

pg8000.native.Connection.notifications
``````````````````````````````````````

A deque of server-side `notifications
<https://www.postgresql.org/docs/current/sql-notify.html>`__ received by this database
connection (via the ``LISTEN`` / ``NOTIFY`` PostgreSQL commands). Each list item is a
three-element tuple containing the PostgreSQL backend PID that issued the notify, the
channel and the payload.


pg8000.native.Connection.notices
````````````````````````````````

A deque of server-side notices received by this database connection.


pg8000.native.Connection.parameter_statuses
```````````````````````````````````````````

A deque of server-side parameter statuses received by this database connection.


pg8000.native.Connection.run(sql, stream=None, types=None, \*\*kwargs)
``````````````````````````````````````````````````````````````````````

Executes an sql statement, and returns the results as a ``list``. For example::

  con.run("SELECT * FROM cities where population > :pop", pop=10000)

sql
  The SQL statement to execute. Parameter placeholders appear as a ``:`` followed by the
  parameter name.

stream
  For use with the PostgreSQL `COPY
  <http://www.postgresql.org/docs/current/static/sql-copy.html>`__ command. The nature
  of the parameter depends on whether the SQL command is ``COPY FROM`` or ``COPY TO``.

  ``COPY FROM``
    The stream parameter must be a readable file-like object or an iterable. If it's an
    iterable then the items can be ``str`` or binary.
  ``COPY TO``
    The stream parameter must be a writable file-like object.

types
  A dictionary of oids. A key corresponds to a parameter. 

kwargs
  The parameters of the SQL statement.


pg8000.native.Connection.row_count
``````````````````````````````````

This read-only attribute contains the number of rows that the last ``run()`` method
produced (for query statements like ``SELECT``) or affected (for modification statements
like ``UPDATE``.

The value is -1 if:

- No ``run()`` method has been performed yet.
- There was no rowcount associated with the last ``run()``.


pg8000.native.Connection.columns
````````````````````````````````

A list of column metadata. Each item in the list is a dictionary with the following
keys:

- name
- table_oid
- column_attrnum
- type_oid
- type_size
- type_modifier
- format


pg8000.native.Connection.close()
````````````````````````````````

Closes the database connection.


pg8000.native.Connection.register_out_adapter(typ, out_func)
````````````````````````````````````````````````````````````

Register a type adapter for types going out from pg8000 to the server.

typ
  The Python class that the adapter is for.

out_func
  A function that takes the Python object and returns its string representation
  in the format that the server requires.


pg8000.native.Connection.register_in_adapter(oid, in_func)
``````````````````````````````````````````````````````````

Register a type adapter for types coming in from the server to pg8000.

oid
  The PostgreSQL type identifier found in the `pg_type system catalog
  <https://www.postgresql.org/docs/current/catalog-pg-type.html>`_.

in_func
  A function that takes the PostgreSQL string representation and returns a corresponding
  Python object.


pg8000.native.Connection.prepare(sql)
`````````````````````````````````````

Returns a ``PreparedStatement`` object which represents a `prepared statement
<https://www.postgresql.org/docs/current/sql-prepare.html>`_ on the server. It can
subsequently be repeatedly executed.

sql
  The SQL statement to prepare. Parameter placeholders appear as a ``:`` followed by the
  parameter name.


pg8000.native.PreparedStatement
```````````````````````````````

A prepared statement object is returned by the ``pg8000.native.Connection.prepare()``
method of a connection. It has the following methods:


pg8000.native.PreparedStatement.run(\*\*kwargs)
```````````````````````````````````````````````

Executes the prepared statement, and returns the results as a ``tuple``.

kwargs
  The parameters of the prepared statement.


pg8000.native.PreparedStatement.close()
```````````````````````````````````````

Closes the prepared statement, releasing the prepared statement held on the server.


pg8000.native.identifier(ident)
```````````````````````````````

Correctly quotes and escapes a string to be used as an `SQL identifier
<https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-IDENTIFIERS>`_.

ident
  The ``str`` to be used as an SQL identifier.


pg8000.native.literal(value)
````````````````````````````

Correctly quotes and escapes a value to be used as an `SQL literal
<https://www.postgresql.org/docs/current/sql-syntax-lexical.html#SQL-SYNTAX-CONSTANTS>`_.

value
  The value to be used as an SQL literal.


DB-API 2 Docs
-------------


Properties
``````````


pg8000.dbapi.apilevel
:::::::::::::::::::::

The DBAPI level supported, currently "2.0".


pg8000.dbapi.threadsafety
:::::::::::::::::::::::::

Integer constant stating the level of thread safety the DBAPI interface supports. For
pg8000, the threadsafety value is 1, meaning that threads may share the module but not
connections.


pg8000.dbapi.paramstyle
:::::::::::::::::::::::

String property stating the type of parameter marker formatting expected by
the interface.  This value defaults to "format", in which parameters are
marked in this format: "WHERE name=%s".

As an extension to the DBAPI specification, this value is not constant; it can be
changed to any of the following values:

qmark
  Question mark style, eg. ``WHERE name=?``

numeric
  Numeric positional style, eg. ``WHERE name=:1``

named
  Named style, eg. ``WHERE name=:paramname``

format
  printf format codes, eg. ``WHERE name=%s``

pyformat
  Python format codes, eg. ``WHERE name=%(paramname)s``


pg8000.dbapi.STRING
:::::::::::::::::::

String type oid.

pg8000.dbapi.BINARY
:::::::::::::::::::


pg8000.dbapi.NUMBER
:::::::::::::::::::

Numeric type oid.


pg8000.dbapi.DATETIME
:::::::::::::::::::::

Timestamp type oid


pg8000.dbapi.ROWID
::::::::::::::::::

ROWID type oid


Functions
`````````

pg8000.dbapi.connect(user, host='localhost', database=None, port=5432, password=None, source_address=None, unix_sock=None, ssl_context=None, timeout=None, tcp_keepalive=True, application_name=None, replication=None, sock=None)
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Creates a connection to a PostgreSQL database.

user
  The username to connect to the PostgreSQL server with. If your server character
  encoding is not ``ascii`` or ``utf8``, then you need to provide ``user`` as bytes,
  eg. ``'my_name'.encode('EUC-JP')``.

host
  The hostname of the PostgreSQL server to connect with. Providing this parameter is
  necessary for TCP/IP connections. One of either ``host`` or ``unix_sock`` must be
  provided. The default is ``localhost``.

database
  The name of the database instance to connect with. If ``None`` then the PostgreSQL
  server will assume the database name is the same as the username. If your server
  character encoding is not ``ascii`` or ``utf8``, then you need to provide ``database``
  as bytes, eg. ``'my_db'.encode('EUC-JP')``.

port
  The TCP/IP port of the PostgreSQL server instance.  This parameter defaults to
  ``5432``, the registered common port of PostgreSQL TCP/IP servers.

password
  The user password to connect to the server with. This parameter is optional; if
  omitted and the database server requests password-based authentication, the
  connection will fail to open. If this parameter is provided but not requested by the
  server, no error will occur.

  If your server character encoding is not ``ascii`` or ``utf8``, then you need to
  provide ``password`` as bytes, eg.  ``'my_password'.encode('EUC-JP')``.

source_address
  The source IP address which initiates the connection to the PostgreSQL server. The
  default is ``None`` which means that the operating system will choose the source
  address.

unix_sock
  The path to the UNIX socket to access the database through, for example,
  ``'/tmp/.s.PGSQL.5432'``. One of either ``host`` or ``unix_sock`` must be provided.

ssl_context
  This governs SSL encryption for TCP/IP sockets. It can have three values:

  - ``None``, meaning no SSL (the default)
  - ``True``, means use SSL with an |ssl.SSLContext|_ created using
    |ssl.create_default_context()|_.

  - An instance of |ssl.SSLContext|_ which will be used to create the SSL connection.

  If your PostgreSQL server is behind an SSL proxy, you can set the pg8000-specific
  attribute ``ssl.SSLContext.request_ssl = False``, which tells pg8000 to use an SSL
  socket, but not to request SSL from the PostgreSQL server. Note that this means you
  can't use SCRAM authentication with channel binding.

timeout
  This is the time in seconds before the connection to the server will time out. The
  default is ``None`` which means no timeout.

tcp_keepalive
  If ``True`` then use `TCP keepalive
  <https://en.wikipedia.org/wiki/Keepalive#TCP_keepalive>`_. The default is ``True``.

application_name
  Sets the `application_name
  <https://www.postgresql.org/docs/current/runtime-config-logging.html#GUC-APPLICATION-NAME>`_. If your server character encoding is not ``ascii`` or ``utf8``, then you need to
  provide values as bytes, eg. ``'my_application_name'.encode('EUC-JP')``. The default
  is ``None`` which means that the server will set the application name.

replication
  Used to run in `streaming replication mode
  <https://www.postgresql.org/docs/current/protocol-replication.html>`_. If your server
  character encoding is not ``ascii`` or ``utf8``, then you need to provide values as
  bytes, eg. ``'database'.encode('EUC-JP')``.

sock
  A socket-like object to use for the connection. For example, ``sock`` could be a plain
  ``socket.socket``, or it could represent an SSH tunnel or perhaps an
  ``ssl.SSLSocket`` to an SSL proxy. If an |ssl.SSLContext| is provided, then it will be
  used to attempt to create an SSL socket from the provided socket. 


pg8000.dbapi.Date(year, month, day)

Construct an object holding a date value.

This property is part of the `DBAPI 2.0 specification
<http://www.python.org/dev/peps/pep-0249/>`_.

Returns: `datetime.date`


pg8000.dbapi.Time(hour, minute, second)
:::::::::::::::::::::::::::::::::::::::

Construct an object holding a time value.

Returns: ``datetime.time``


pg8000.dbapi.Timestamp(year, month, day, hour, minute, second)
::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Construct an object holding a timestamp value.

Returns: ``datetime.datetime``


pg8000.dbapi.DateFromTicks(ticks)
:::::::::::::::::::::::::::::::::

Construct an object holding a date value from the given ticks value (number of seconds
since the epoch).

Returns: ``datetime.datetime``


pg8000.dbapi.TimeFromTicks(ticks)
:::::::::::::::::::::::::::::::::

Construct an object holding a time value from the given ticks value (number of seconds
since the epoch).

Returns: ``datetime.time``


pg8000.dbapi.TimestampFromTicks(ticks)
::::::::::::::::::::::::::::::::::::::

Construct an object holding a timestamp value from the given ticks value (number of
seconds since the epoch).

Returns: ``datetime.datetime``


pg8000.dbapi.Binary(value)
::::::::::::::::::::::::::

Construct an object holding binary data.

Returns: ``bytes``.


Generic Exceptions
``````````````````

Pg8000 uses the standard DBAPI 2.0 exception tree as "generic" exceptions. Generally,
more specific exception types are raised; these specific exception types are derived
from the generic exceptions.

pg8000.dbapi.Warning
::::::::::::::::::::

Generic exception raised for important database warnings like data truncations. This
exception is not currently used by pg8000.


pg8000.dbapi.Error
::::::::::::::::::

Generic exception that is the base exception of all other error exceptions.


pg8000.dbapi.InterfaceError
:::::::::::::::::::::::::::

Generic exception raised for errors that are related to the database interface rather
than the database itself. For example, if the interface attempts to use an SSL
connection but the server refuses, an InterfaceError will be raised.


pg8000.dbapi.DatabaseError
::::::::::::::::::::::::::

Generic exception raised for errors that are related to the database. This exception is
currently never raised by pg8000.


pg8000.dbapi.DataError
::::::::::::::::::::::

Generic exception raised for errors that are due to problems with the processed data.
This exception is not currently raised by pg8000.


pg8000.dbapi.OperationalError
:::::::::::::::::::::::::::::

Generic exception raised for errors that are related to the database's operation and not
necessarily under the control of the programmer. This exception is currently never
raised by pg8000.


pg8000.dbapi.IntegrityError
:::::::::::::::::::::::::::

Generic exception raised when the relational integrity of the database is affected. This
exception is not currently raised by pg8000.


pg8000.dbapi.InternalError
::::::::::::::::::::::::::

Generic exception raised when the database encounters an internal error. This is
currently only raised when unexpected state occurs in the pg8000 interface itself, and
is typically the result of a interface bug.


pg8000.dbapi.ProgrammingError
:::::::::::::::::::::::::::::

Generic exception raised for programming errors. For example, this exception is raised
if more parameter fields are in a query string than there are available parameters.


pg8000.dbapi.NotSupportedError
::::::::::::::::::::::::::::::

Generic exception raised in case a method or database API was used which is not
supported by the database.


Classes
```````


pg8000.dbapi.Connection
:::::::::::::::::::::::

A connection object is returned by the ``pg8000.connect()`` function. It represents a
single physical connection to a PostgreSQL database.


pg8000.dbapi.Connection.autocommit
::::::::::::::::::::::::::::::::::

Following the DB-API specification, autocommit is off by default. It can be turned on by
setting this boolean pg8000-specific autocommit property to ``True``.


pg8000.dbapi.Connection.close()
:::::::::::::::::::::::::::::::

Closes the database connection.


pg8000.dbapi.Connection.cursor()
::::::::::::::::::::::::::::::::

Creates a ``pg8000.dbapi.Cursor`` object bound to this connection.


pg8000.dbapi.Connection.rollback()
::::::::::::::::::::::::::::::::::

Rolls back the current database transaction.


pg8000.dbapi.Connection.tpc_begin(xid)
::::::::::::::::::::::::::::::::::::::

Begins a TPC transaction with the given transaction ID xid. This method should be
called outside of a transaction (i.e. nothing may have executed since the last
``commit()``  or ``rollback()``. Furthermore, it is an error to call ``commit()`` or
``rollback()`` within the TPC transaction. A ``ProgrammingError`` is raised, if the
application calls ``commit()`` or ``rollback()`` during an active TPC transaction.


pg8000.dbapi.Connection.tpc_commit(xid=None)
::::::::::::::::::::::::::::::::::::::::::::

When called with no arguments, ``tpc_commit()`` commits a TPC transaction previously
prepared with ``tpc_prepare()``. If ``tpc_commit()`` is called prior to
``tpc_prepare()``, a single phase commit is performed. A transaction manager may choose
to do this if only a single resource is participating in the global transaction.

When called with a transaction ID ``xid``, the database commits the given transaction.
If an invalid transaction ID is provided, a ``ProgrammingError`` will be raised. This
form should be called outside of a transaction, and is intended for use in recovery.

On return, the TPC transaction is ended.


pg8000.dbapi.Connection.tpc_prepare()
:::::::::::::::::::::::::::::::::::::

Performs the first phase of a transaction started with ``.tpc_begin()``. A
``ProgrammingError`` is be raised if this method is called outside of a TPC transaction.

After calling ``tpc_prepare()``, no statements can be executed until ``tpc_commit()`` or
``tpc_rollback()`` have been called.


pg8000.dbapi.Connection.tpc_recover()
:::::::::::::::::::::::::::::::::::::

Returns a list of pending transaction IDs suitable for use with ``tpc_commit(xid)`` or
``tpc_rollback(xid)``.


pg8000.dbapi.Connection.tpc_rollback(xid=None)
::::::::::::::::::::::::::::::::::::::::::::::

When called with no arguments, ``tpc_rollback()`` rolls back a TPC transaction. It may
be called before or after ``tpc_prepare()``.

When called with a transaction ID xid, it rolls back the given transaction. If an
invalid transaction ID is provided, a ``ProgrammingError`` is raised. This form should
be called outside of a transaction, and is intended for use in recovery.

On return, the TPC transaction is ended.


pg8000.dbapi.Connection.xid(format_id, global_transaction_id, branch_qualifier)
:::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::

Create a Transaction IDs (only global_transaction_id is used in pg) format_id and
branch_qualifier are not used in postgres global_transaction_id may be any string
identifier supported by postgres returns a tuple (format_id, global_transaction_id,
branch_qualifier)


pg8000.dbapi.Cursor
:::::::::::::::::::

A cursor object is returned by the ``pg8000.dbapi.Connection.cursor()`` method of a
connection. It has the following attributes and methods:

pg8000.dbapi.Cursor.arraysize
'''''''''''''''''''''''''''''

This read/write attribute specifies the number of rows to fetch at a time with
``pg8000.dbapi.Cursor.fetchmany()``.  It defaults to 1.


pg8000.dbapi.Cursor.connection
''''''''''''''''''''''''''''''

This read-only attribute contains a reference to the connection object (an instance of
``pg8000.dbapi.Connection``) on which the cursor was created.


pg8000.dbapi.Cursor.rowcount
''''''''''''''''''''''''''''

This read-only attribute contains the number of rows that the last ``execute()`` or
``executemany()`` method produced (for query statements like ``SELECT``) or affected
(for modification statements like ``UPDATE``.

The value is -1 if:

- No ``execute()`` or ``executemany()`` method has been performed yet on the cursor.

- There was no rowcount associated with the last ``execute()``.

- At least one of the statements executed as part of an ``executemany()`` had no row
  count associated with it.


pg8000.dbapi.Cursor.description
'''''''''''''''''''''''''''''''

This read-only attribute is a sequence of 7-item sequences. Each value contains
information describing one result column. The 7 items returned for each column are
(name, type_code, display_size, internal_size, precision, scale, null_ok). Only the
first two values are provided by the current implementation.


pg8000.dbapi.Cursor.close()
'''''''''''''''''''''''''''

Closes the cursor.


pg8000.dbapi.Cursor.execute(operation, args=None, stream=None)
''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''

Executes a database operation. Parameters may be provided as a sequence, or as a
mapping, depending upon the value of ``pg8000.dbapi.paramstyle``. Returns the cursor,
which may be iterated over.

operation
  The SQL statement to execute.

args
  If ``pg8000.dbapi.paramstyle`` is ``qmark``, ``numeric``, or ``format``, this
  argument should be an array of parameters to bind into the statement. If
  ``pg8000.dbapi.paramstyle`` is ``named``, the argument should be a ``dict`` mapping of
  parameters. If ``pg8000.dbapi.paramstyle`` is ``pyformat``, the argument value may be
  either an array or a mapping.

stream
  This is a pg8000 extension for use with the PostgreSQL `COPY
  <http://www.postgresql.org/docs/current/static/sql-copy.html>`__ command. For a
  ``COPY FROM`` the parameter must be a readable file-like object, and for ``COPY TO``
  it must be writable.


pg8000.dbapi.Cursor.executemany(operation, param_sets)
''''''''''''''''''''''''''''''''''''''''''''''''''''''

Prepare a database operation, and then execute it against all parameter sequences or
mappings provided.

operation
  The SQL statement to execute.

parameter_sets
  A sequence of parameters to execute the statement with. The values in the sequence
  should be sequences or mappings of parameters, the same as the args argument of the
  ``pg8000.dbapi.Cursor.execute()`` method.


pg8000.dbapi.Cursor.callproc(procname, parameters=None)
'''''''''''''''''''''''''''''''''''''''''''''''''''''''

Call a stored database procedure with the given name and optional parameters.


procname
  The name of the procedure to call.

parameters
  A list of parameters.


pg8000.dbapi.Cursor.fetchall()
''''''''''''''''''''''''''''''

Fetches all remaining rows of a query result.

Returns: A sequence, each entry of which is a sequence of field values making up a row.


pg8000.dbapi.Cursor.fetchmany(size=None)
''''''''''''''''''''''''''''''''''''''''

Fetches the next set of rows of a query result.

size
  The number of rows to fetch when called.  If not provided, the
  ``pg8000.dbapi.Cursor.arraysize`` attribute value is used instead.

Returns: A sequence, each entry of which is a sequence of field values making up a row.
If no more rows are available, an empty sequence will be returned.


pg8000.dbapi.Cursor.fetchone()
''''''''''''''''''''''''''''''

Fetch the next row of a query result set.

Returns: A row as a sequence of field values, or ``None`` if no more rows are available.


pg8000.dbapi.Cursor.setinputsizes(\*sizes)
''''''''''''''''''''''''''''''''''''''''''

Used to set the parameter types of the next query. This is useful if it's difficult for
pg8000 to work out the types from the parameters themselves (eg. for parameters of type
None).

sizes
  Positional parameters that are either the Python type of the parameter to be sent, or
  the PostgreSQL oid. Common oids are available as constants such as ``pg8000.STRING``,
  ``pg8000.INTEGER``, ``pg8000.TIME`` etc.


pg8000.dbapi.Cursor.setoutputsize(size, column=None)
''''''''''''''''''''''''''''''''''''''''''''''''''''

Not implemented by pg8000.


pg8000.dbapi.Interval
'''''''''''''''''''''

An Interval represents a measurement of time.  In PostgreSQL, an interval is defined in
the measure of months, days, and microseconds; as such, the pg8000 interval type
represents the same information.

Note that values of the ``pg8000.dbapi.Interval.microseconds``,
``pg8000.dbapi.Interval.days``, and ``pg8000.dbapi.Interval.months`` properties are
independently measured and cannot be converted to each other. A month may be 28, 29, 30,
or 31 days, and a day may occasionally be lengthened slightly by a leap second.


Design Decisions
----------------

For the ``Range`` type, the constructor follows the `PostgreSQL range constructor functions <https://www.postgresql.org/docs/current/rangetypes.html#RANGETYPES-CONSTRUCT>`_
which makes `[closed, open) <https://fhur.me/posts/always-use-closed-open-intervals>`_
the easiest to express:

>>> from pg8000.types import Range
>>>
>>> pg_range = Range(2, 6)


Tests
-----

- Install `tox <http://testrun.org/tox/latest/>`_: ``pip install tox``

- Enable the PostgreSQL hstore extension by running the SQL command:
  ``create extension hstore;``

- Add a line to ``pg_hba.conf`` for the various authentication options:

::

  host    pg8000_md5           all        127.0.0.1/32            md5
  host    pg8000_gss           all        127.0.0.1/32            gss
  host    pg8000_password      all        127.0.0.1/32            password
  host    pg8000_scram_sha_256 all        127.0.0.1/32            scram-sha-256
  host    all                  all        127.0.0.1/32            trust

- Set password encryption to ``scram-sha-256`` in ``postgresql.conf``:
  ``password_encryption = 'scram-sha-256'``

- Set the password for the postgres user: ``ALTER USER postgresql WITH PASSWORD 'pw';``

- Run ``tox`` from the ``pg8000`` directory: ``tox``

This will run the tests against the Python version of the virtual environment, on the
machine, and the installed PostgreSQL version listening on port 5432, or the ``PGPORT``
environment variable if set.

Benchmarks are run as part of the test suite at ``tests/test_benchmarks.py``.


README.rst
----------

This file is written in the `reStructuredText
<https://docutils.sourceforge.io/docs/user/rst/quickref.html>`_ format. To generate an
HTML page from it, do:

- Activate the virtual environment: ``source venv/bin/activate``
- Install ``Sphinx``: ``pip install Sphinx``
- Run ``rst2html.py``: ``rst2html.py README.rst README.html``


Doing A Release Of pg8000
-------------------------

Run ``tox`` to make sure all tests pass, then update the release notes, then do:

::

  git tag -a x.y.z -m "version x.y.z"
  rm -r dist
  python -m build
  twine upload dist/*


Release Notes
-------------

Version 1.30.3, 2023-10-31
``````````````````````````

- Fix problem with PG date overflowing Python types. Now we return the ``str`` we got from the
  server if we can't parse it. 


Version 1.30.2, 2023-09-17
``````````````````````````

- Bug fix where dollar-quoted string constants weren't supported.


Version 1.30.1, 2023-07-29
``````````````````````````

- There was a problem uploading the previous version (1.30.0) to PyPI because the
  markup of the README.rst was invalid. There's now a step in the automated tests to
  check for this.


Version 1.30.0, 2023-07-27
``````````````````````````

- Remove support for Python 3.7

- Add a ``sock`` keyword parameter for creating a connection from a pre-configured
  socket.


Version 1.29.8, 2023-06-16
``````````````````````````

- Ranges don't work with legacy API.


Version 1.29.7, 2023-06-16
``````````````````````````

- Add support for PostgreSQL ``range`` and ``multirange`` types. Previously pg8000
  would just return them as strings, but now they're returned as ``Range`` and lists of
  ``Range``.

- The PostgreSQL ``record`` type is now returned as a ``tuple`` of strings, whereas
  before it was returned as one string.


Version 1.29.6, 2023-05-29
``````````````````````````

- Fixed two bugs with composite types. Nulls should be represented by an empty string,
  and in an array of composite types, the elements should be surrounded by double
  quotes.


Version 1.29.5, 2023-05-09
``````````````````````````

- Fixed bug where pg8000 didn't handle the case when the number of bytes received from
  a socket was fewer than requested. This was being interpreted as a network error, but
  in fact we just needed to wait until more bytes were available.

- When using the ``PGInterval`` type, if a response from the server contained the period
  ``millennium``, it wasn't recognised. This was caused by a spelling mistake where we
  had ``millenium`` rather than ``millennium``.

- Added support for sending PostgreSQL composite types. If a value is sent as a
  ``tuple``, pg8000 will send it to the server as a ``(`` delimited composite string.


Version 1.29.4, 2022-12-14
``````````````````````````

- Fixed bug in ``pg8000.dbapi`` in the ``setinputsizes()`` method where if a ``size``
  was a recognized Python type, the method failed.


Version 1.29.3, 2022-10-26
``````````````````````````

- Upgrade the SCRAM library to version 1.4.3. This adds support for the case where the
  client supports channel binding but the server doesn't.


Version 1.29.2, 2022-10-09
``````````````````````````

- Fixed a bug where in a literal array, items such as ``\n`` and ``\r`` weren't
  escaped properly before being sent to the server.

- Fixed a bug where if the PostgreSQL server has a half-hour time zone set, values of
  type ``timestamp with time zone`` failed. This has been fixed by using the ``parse``
  function of the ``dateutil`` package if the ``datetime`` parser fails.


Version 1.29.1, 2022-05-23
``````````````````````````

- In trying to determine if there's been a failed commit, check for ``ROLLBACK TO
  SAVEPOINT``.


Version 1.29.0, 2022-05-21
``````````````````````````

- Implement a workaround for the `silent failed commit
  <https://github.com/tlocke/pg8000/issues/36>`_ bug.

- Previously if an empty string was sent as the query an exception would be raised, but
  that isn't done now.


Version 1.28.3, 2022-05-18
``````````````````````````

- Put back ``__version__`` attributes that were inadvertently removed.


Version 1.28.2, 2022-05-17
``````````````````````````

- Use a build system that's compliant with PEP517.


Version 1.28.1, 2022-05-17
``````````````````````````

- If when doing a ``COPY FROM`` the ``stream`` parameter is an iterator of ``str``,
  pg8000 used to silently append a newline to the end. That no longer happens.


Version 1.28.0, 2022-05-17
``````````````````````````

- When using the ``COPY FROM`` SQL statement, allow the ``stream`` parameter to be an
  iterable.


Version 1.27.1, 2022-05-16
``````````````````````````

- The ``seconds`` attribute of ``PGInterval`` is now always a ``float``, to cope with
  fractional seconds.

- Updated the ``interval`` parsers for ``iso_8601`` and ``sql_standard`` to take
  account of fractional seconds.


Version 1.27.0, 2022-05-16
``````````````````````````

- It used to be that by default, if pg8000 received an ``interval`` type from the server
  and it was too big to fit into a ``datetime.timedelta`` then an exception would be
  raised. Now if an interval is too big for ``datetime.timedelta`` a ``PGInterval`` is
  returned.

* pg8000 now supports all the output formats for an ``interval`` (``postgres``,
  ``postgres_verbose``, ``iso_8601`` and ``sql_standard``).


Version 1.26.1, 2022-04-23
``````````````````````````

- Make sure all tests are run by the GitHub Actions tests on commit.
- Remove support for Python 3.6
- Remove support for PostgreSQL 9.6


Version 1.26.0, 2022-04-18
``````````````````````````

- When connecting, raise an ``InterfaceError('network error')`` rather than let the
  underlying ``struct.error`` float up.

- Make licence text the same as that used by the OSI. Previously the licence wording
  differed slightly from the BSD 3 Clause licence at
  https://opensource.org/licenses/BSD-3-Clause. This meant that automated tools didn't
  pick it up as being Open Source. The changes are believed to not alter the meaning of   the license at all.


Version 1.25.0, 2022-04-17
``````````````````````````

- Fix more cases where a ``ResourceWarning`` would be raise because of a socket that had
  been left open.

- We now have a single ``InterfaceError`` with the message 'network error' for all
  network errors, with the underlying exception held in the ``cause`` of the exception.


Version 1.24.2, 2022-04-15
``````````````````````````

- To prevent a ``ResourceWarning`` close socket if a connection can't be created.


Version 1.24.1, 2022-03-02
``````````````````````````

- Return pg +/-infinity dates as ``str``. Previously +/-infinity pg values would cause
  an error when returned, but now we return +/-infinity as strings.


Version 1.24.0, 2022-02-06
``````````````````````````

- Add SQL escape functions identifier() and literal() to the native API. For use when a
  query can't be parameterised and the SQL string has to be created using untrusted
  values.


Version 1.23.0, 2021-11-13
``````````````````````````

- If a query has no parameters, then the query will no longer be parsed. Although there
  are performance benefits for doing this, the main reason is to avoid query rewriting,
  which can introduce errors.


Version 1.22.1, 2021-11-10
``````````````````````````

- Fix bug in PGInterval type where ``str()`` failed for a millennia value.


Version 1.22.0, 2021-10-13
``````````````````````````

- Rather than specifying the oids in the ``Parse`` step of the Postgres protocol, pg8000
  now omits them, and so Postgres will use the oids it determines from the query. This
  makes the pg8000 code simpler and also it should also make the nuances of type
  matching more straightforward.
