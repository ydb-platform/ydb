# Building the JDBC driver for {{ ydb-short-name }}

To execute all tests in the project, run the `mvn test` command.

By default, all tests are run using a local {{ ydb-short-name }} instance in Docker (if the host has Docker or Docker Machine installed).

To disable these tests, run: `mvn test -DYDB_DISABLE_INTEGRATION_TESTS=true`