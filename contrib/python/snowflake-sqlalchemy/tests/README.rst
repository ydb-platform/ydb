Building and Testing Snowflake SQLAlchemy
********************************************************************************

Building
================================================================================

Install Python 3.5.0 or higher. Clone the Snowflake SQLAlchemy repository, then run the following command to create a wheel package:

    .. code-block:: bash

        git clone git@github.com:snowflakedb/snowflake-sqlalchemy.git
        cd snowflake-sqlalchemy
        pyvenv /tmp/test_snowflake_sqlalchemy
        source /tmp/test_snowflake_sqlalchemy/bin/activate
        pip install -U pip setuptools wheel
        python setup.py bdist_wheel

Find the ``snowflake-sqlalchemy*.whl`` package in the ``./dist`` directory.


Testing
================================================================================

Create a virtualenv, with ``parameters.py`` in a test directory.

    .. code-block:: bash

        pyvenv /tmp/test_snowflake_sqlalchemy
        source /tmp/test_snowflake_sqlalchemy/bin/activate
        pip install Cython
        pip install pytest numpy pandas
        pip install dist/snowflake_sqlalchemy*.whl
        vim tests/parameters.py

In the ``parameters.py`` file, include the connection information in a Python dictionary.

    .. code-block:: python

        CONNECTION_PARAMETERS = {
            'account':  'testaccount',
            'user':     'user1',
            'password': 'testpasswd',
            'schema':   'testschema',
            'database': 'testdb',
        }

Run the test:

    .. code-block:: bash

        py.test test
