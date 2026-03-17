xx§§x§# Building and Testing Snowflake Connector for Python

## Running tests

Place the `parameters.py` file in the `test` directory, with the connection information in a Python dictionary:

```python
CONNECTION_PARAMETERS = {
    'account':  'testaccount',
    'user':     'user',
    'password': 'testpasswd',
    'schema':   'testschema',
    'database': 'testdb',
}
```

### Running a single test

Assuming that all dependencies are installed, running a single test is as simple as:
`python -m pytest test/integ/test_connection.py::test_basic`.

### Running a suite of tests

We use `tox` to run test suites and other utilities.

To run the most important tests, execute:

```shell
tox -e "fix_lint,py37{,-pandas,-sso}"
```

## Test types
These test types can be mixed with test categories, or with each other.
Note: providing both to tox runs both integration and unit tests of the current category and not providing
either does the same as providing both of them.

* **integ**: Integration tests that need to connect to a Snowflake environment.
* **unit**: Unit tests that can run locally, but they might still require internet connection.

## Test categories
Chaining these categories is possible, but isn't encouraged.
Note: running multiple categories in one `tox` run should be done like:
`tox -e "fix_lint,py37-{,-sso},coverage"`

* **pandas**: Tests specifically testing our optional dependency group "pandas".
* **sso**: Tests specifically testing our optional dependency group "sso".
* **extras**: Tests special cases under separate processes.

Special categories:
* **skipolddriver**: We run the newest tests on the oldest still supported Python connector to verify that they
still work. However; some behaviors change over time and new features get added. For this reason tests tagged with
this marker will not run with old driver version. Any tests that verify new behavior, or old tests that are changed
to use new features should have this marker on them.

## Other test tags
* **internal**: Tests that should only be run on our internal CI.
* **external**: Tests that should only be run on our external CI.
