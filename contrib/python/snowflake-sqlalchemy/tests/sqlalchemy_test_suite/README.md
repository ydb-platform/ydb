# SQLAlchemy Compliance Tests

SQLAlchemy offers tests to test SQLAlchemy dialects work properly. This directory applies these tests
to the Snowflake SQLAlchemy dialect.

**Please be aware that the test suites are not collected in pytest by default** -- the directory is ignored in `tox.ini`.
There are majorly three issues with the sqlalchemy test suites:
1. Importing sqlalchemy pytest plugin will result in Snowflake SQLAlchemy dialect specific tests not
being collected.
2. Putting test_suites.py and related conftest config under root tests dir would make pytest fail to collect cfg
information such as requirements, and customized session db config does not work as expected (schema not set).

Running sqlalchemy test suites from a separate directory does not have the aforementioned issues. Thus, we skip the
path in the config, and run test suites separately.

To run the SQLAlchemy test suites, please specify the directory of the test suites when running pytest command:

```bash
$cd snowflake-sqlalchemy
$pytest tests/sqlalchemy_test_suite
```
