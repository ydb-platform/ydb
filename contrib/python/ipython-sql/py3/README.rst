===========
ipython-sql
===========

:Author: Catherine Devlin, http://catherinedevlin.blogspot.com

Introduces a %sql (or %%sql) magic.

Connect to a database, using `SQLAlchemy URL`_ connect strings, then issue SQL
commands within IPython or IPython Notebook.

.. image:: https://raw.github.com/catherinedevlin/ipython-sql/master/examples/writers.png
   :width: 600px
   :alt: screenshot of ipython-sql in the Notebook

Examples
--------

.. code-block:: python

    In [1]: %load_ext sql

    In [2]: %%sql postgresql://will:longliveliz@localhost/shakes
       ...: select * from character
       ...: where abbrev = 'ALICE'
       ...:
    Out[2]: [(u'Alice', u'Alice', u'ALICE', u'a lady attending on Princess Katherine', 22)]

    In [3]: result = _

    In [4]: print(result)
    charid   charname   abbrev                description                 speechcount
    =================================================================================
    Alice    Alice      ALICE    a lady attending on Princess Katherine   22

    In [4]: result.keys
    Out[5]: [u'charid', u'charname', u'abbrev', u'description', u'speechcount']

    In [6]: result[0][0]
    Out[6]: u'Alice'

    In [7]: result[0].description
    Out[7]: u'a lady attending on Princess Katherine'

After the first connection, connect info can be omitted::

    In [8]: %sql select count(*) from work
    Out[8]: [(43L,)]

Connections to multiple databases can be maintained.  You can refer to
an existing connection by username@database

.. code-block:: python

    In [9]: %%sql will@shakes
       ...: select charname, speechcount from character
       ...: where  speechcount = (select max(speechcount)
       ...:                       from character);
       ...:
    Out[9]: [(u'Poet', 733)]

    In [10]: print(_)
    charname   speechcount
    ======================
    Poet       733

If no connect string is supplied, ``%sql`` will provide a list of existing connections;
however, if no connections have yet been made and the environment variable ``DATABASE_URL``
is available, that will be used.

For secure access, you may dynamically access your credentials (e.g. from your system environment or `getpass.getpass`) to avoid storing your password in the notebook itself. Use the `$` before any variable to access it in your `%sql` command.

.. code-block:: python

    In [11]: user = os.getenv('SOME_USER')
       ....: password = os.getenv('SOME_PASSWORD')
       ....: connection_string = "postgresql://{user}:{password}@localhost/some_database".format(user=user, password=password)
       ....: %sql $connection_string
    Out[11]: u'Connected: some_user@some_database'

You may use multiple SQL statements inside a single cell, but you will
only see any query results from the last of them, so this really only
makes sense for statements with no output

.. code-block:: python

    In [11]: %%sql sqlite://
       ....: CREATE TABLE writer (first_name, last_name, year_of_death);
       ....: INSERT INTO writer VALUES ('William', 'Shakespeare', 1616);
       ....: INSERT INTO writer VALUES ('Bertold', 'Brecht', 1956);
       ....:
    Out[11]: []


As a convenience, dict-style access for result sets is supported, with the
leftmost column serving as key, for unique values.

.. code-block:: python

    In [12]: result = %sql select * from work
    43 rows affected.

    In [13]: result['richard2']
    Out[14]: (u'richard2', u'Richard II', u'History of Richard II', 1595, u'h', None, u'Moby', 22411, 628)

Results can also be retrieved as an iterator of dictionaries (``result.dicts()``)
or a single dictionary with a tuple of scalar values per key (``result.dict()``)

Variable substitution 
---------------------

Bind variables (bind parameters) can be used in the "named" (:x) style.
The variable names used should be defined in the local namespace.

.. code-block:: python

    In [15]: name = 'Countess'

    In [16]: %sql select description from character where charname = :name
    Out[16]: [(u'mother to Bertram',)]

Alternately, ``$variable_name`` or ``{variable_name}`` can be 
used to inject variables from the local namespace into the SQL 
statement before it is formed and passed to the SQL engine.
(Using ``$`` and ``{}`` together, as in ``${variable_name}``, 
is not supported.)


    In [17]: %sql select description from character where charname = '{name}' 
    Out[17]: [(u'mother to Bertram',)]

Bind variables are passed through to the SQL engine and can only 
be used to replace strings passed to SQL.  ``$`` and ``{}`` are 
substituted before passing to SQL and can be used to form SQL 
statements dynamically.

Assignment
----------

Ordinary IPython assignment works for single-line `%sql` queries:

.. code-block:: python

    In [18]: works = %sql SELECT title, year FROM work
    43 rows affected.

The `<<` operator captures query results in a local variable, and
can be used in multi-line ``%%sql``:

.. code-block:: python

    In [19]: %%sql works << SELECT title, year
        ...: FROM work
        ...:
    43 rows affected.
    Returning data to local variable works

Connecting
----------

Connection strings are `SQLAlchemy URL`_ standard.

Some example connection strings::

    mysql+pymysql://scott:tiger@localhost/foo
    oracle://scott:tiger@127.0.0.1:1521/sidname
    sqlite://
    sqlite:///foo.db
    mssql+pyodbc://username:password@host/database?driver=SQL+Server+Native+Client+11.0

.. _`SQLAlchemy URL`: http://docs.sqlalchemy.org/en/latest/core/engines.html#database-urls

Note that ``mysql`` and ``mysql+pymysql`` connections (and perhaps others)
don't read your client character set information from .my.cnf.  You need
to specify it in the connection string::

    mysql+pymysql://scott:tiger@localhost/foo?charset=utf8

Note that an ``impala`` connection with `impyla`_  for HiveServer2 requires disabling autocommit::

    %config SqlMagic.autocommit=False
    %sql impala://hserverhost:port/default?kerberos_service_name=hive&auth_mechanism=GSSAPI

.. _impyla: https://github.com/cloudera/impyla

Connection arguments not whitelisted by SQLALchemy can be provided as
a flag with (-a|--connection_arguments)the connection string as a JSON string.
See `SQLAlchemy Args`_.

    | %sql --connection_arguments {"timeout":10,"mode":"ro"} sqlite:// SELECT * FROM work;
    | %sql -a '{"timeout":10, "mode":"ro"}' sqlite:// SELECT * from work;

.. _`SQLAlchemy Args`: https://docs.sqlalchemy.org/en/13/core/engines.html#custom-dbapi-args

DSN connections
~~~~~~~~~~~~~~~

Alternately, you can store connection info in a 
configuration file, under a section name chosen to 
refer to your database.

For example, if dsn.ini contains 

    | [DB_CONFIG_1] 
    | drivername=postgres 
    | host=my.remote.host 
    | port=5433 
    | database=mydatabase 
    | username=myuser 
    | password=1234

then you can  

    | %config SqlMagic.dsn_filename='./dsn.ini'
    | %sql --section DB_CONFIG_1 

Configuration
-------------

Query results are loaded as lists, so very large result sets may use up
your system's memory and/or hang your browser.  There is no autolimit
by default.  However, `autolimit` (if set) limits the size of the result
set (usually with a `LIMIT` clause in the SQL).  `displaylimit` is similar,
but the entire result set is still pulled into memory (for later analysis);
only the screen display is truncated.

.. code-block:: python

   In [2]: %config SqlMagic
   SqlMagic options
   --------------
   SqlMagic.autocommit=<Bool>
       Current: True
       Set autocommit mode
   SqlMagic.autolimit=<Int>
       Current: 0
       Automatically limit the size of the returned result sets
   SqlMagic.autopandas=<Bool>
       Current: False
       Return Pandas DataFrames instead of regular result sets
   SqlMagic.column_local_vars=<Bool>
       Current: False
       Return data into local variables from column names
   SqlMagic.displaycon=<Bool>
       Current: False
       Show connection string after execute
   SqlMagic.displaylimit=<Int>
       Current: None
       Automatically limit the number of rows displayed (full result set is still
       stored)
   SqlMagic.dsn_filename=<Unicode>
       Current: 'odbc.ini'
       Path to DSN file. When the first argument is of the form [section], a
       sqlalchemy connection string is formed from the matching section in the DSN
       file.
   SqlMagic.feedback=<Bool>
       Current: False
       Print number of rows affected by DML
   SqlMagic.short_errors=<Bool>
       Current: True
       Don't display the full traceback on SQL Programming Error
   SqlMagic.style=<Unicode>
       Current: 'DEFAULT'
       Set the table printing style to any of prettytable's defined styles
       (currently DEFAULT, MSWORD_FRIENDLY, PLAIN_COLUMNS, RANDOM)

   In[3]: %config SqlMagic.feedback = False

Please note: if you have autopandas set to true, the displaylimit option will not apply. You can set the pandas display limit by using the pandas ``max_rows`` option as described in the `pandas documentation <http://pandas.pydata.org/pandas-docs/version/0.18.1/options.html#frequently-used-options>`_.

Pandas
------

If you have installed ``pandas``, you can use a result set's
``.DataFrame()`` method

.. code-block:: python

    In [3]: result = %sql SELECT * FROM character WHERE speechcount > 25

    In [4]: dataframe = result.DataFrame()


The ``--persist`` argument, with the name of a 
DataFrame object in memory, 
will create a table name
in the database from the named DataFrame.  
Or use ``--append`` to add rows to an existing 
table by that name.

.. code-block:: python

    In [5]: %sql --persist dataframe

    In [6]: %sql SELECT * FROM dataframe;

.. _Pandas: http://pandas.pydata.org/

Graphing
--------

If you have installed ``matplotlib``, you can use a result set's
``.plot()``, ``.pie()``, and ``.bar()`` methods for quick plotting

.. code-block:: python

    In[5]: result = %sql SELECT title, totalwords FROM work WHERE genretype = 'c'

    In[6]: %matplotlib inline

    In[7]: result.pie()

.. image:: https://raw.github.com/catherinedevlin/ipython-sql/master/examples/wordcount.png
   :alt: pie chart of word count of Shakespeare's comedies

Dumping
-------

Result sets come with a ``.csv(filename=None)`` method.  This generates
comma-separated text either as a return value (if ``filename`` is not
specified) or in a file of the given name.

.. code-block:: python

    In[8]: result = %sql SELECT title, totalwords FROM work WHERE genretype = 'c'

    In[9]: result.csv(filename='work.csv')

PostgreSQL features
-------------------

``psql``-style "backslash" `meta-commands`_ commands (``\d``, ``\dt``, etc.)
are provided by `PGSpecial`_.  Example:

.. code-block:: python

    In[9]: %sql \d

.. _PGSpecial: https://pypi.python.org/pypi/pgspecial

.. _meta-commands: https://www.postgresql.org/docs/9.6/static/app-psql.html#APP-PSQL-META-COMMANDS


Options
-------

``-l`` / ``--connections``
    List all active connections

``-x`` / ``--close <session-name>`` 
    Close named connection 

``-c`` / ``--creator <creator-function>``
    Specify creator function for new connection

``-s`` / ``--section <section-name>``
    Section of dsn_file to be used for generating a connection string

``-p`` / ``--persist``
    Create a table name in the database from the named DataFrame

``--append``
    Like ``--persist``, but appends to the table if it already exists 

``-a`` / ``--connection_arguments <"{connection arguments}">``
    Specify dictionary of connection arguments to pass to SQL driver

``-f`` / ``--file <path>``
    Run SQL from file at this path

Caution 
-------

Comments
~~~~~~~~

Because ipyton-sql accepts ``--``-delimited options like ``--persist``, but ``--`` 
is also the syntax to denote a SQL comment, the parser needs to make some assumptions.

- If you try to pass an unsupported argument, like ``--lutefisk``, it will 
  be interpreted as a SQL comment and will not throw an unsupported argument 
  exception.
- If the SQL statement begins with a first-line comment that looks like one 
  of the accepted arguments - like ``%sql --persist is great!`` - it will be 
  parsed like an argument, not a comment.  Moving the comment to the second 
  line or later will avoid this.

Installing
----------

Install the latest release with::

    pip install ipython-sql

or download from https://github.com/catherinedevlin/ipython-sql and::

    cd ipython-sql
    sudo python setup.py install

Development
-----------

https://github.com/catherinedevlin/ipython-sql

Credits
-------

- Matthias Bussonnier for help with configuration
- Olivier Le Thanh Duong for ``%config`` fixes and improvements
- Distribute_
- Buildout_
- modern-package-template_
- Mike Wilson for bind variable code
- Thomas Kluyver and Steve Holden for debugging help
- Berton Earnshaw for DSN connection syntax
- Bruno Harbulot for DSN example 
- Andrés Celis for SQL Server bugfix
- Michael Erasmus for DataFrame truth bugfix
- Noam Finkelstein for README clarification
- Xiaochuan Yu for `<<` operator, syntax colorization
- Amjith Ramanujam for PGSpecial and incorporating it here
- Alexander Maznev for better arg parsing, connections accepting specified creator
- Jonathan Larkin for configurable displaycon 
- Jared Moore for ``connection-arguments`` support
- Gilbert Brault for ``--append`` 
- Lucas Zeer for multi-line bugfixes for var substitution, ``<<`` 
- vkk800 for ``--file``
- Jens Albrecht for MySQL DatabaseError bugfix
- meihkv for connection-closing bugfix
- Abhinav C for SQLAlchemy 2.0 compatibility

.. _Distribute: http://pypi.python.org/pypi/distribute
.. _Buildout: http://www.buildout.org/
.. _modern-package-template: http://pypi.python.org/pypi/modern-package-template
