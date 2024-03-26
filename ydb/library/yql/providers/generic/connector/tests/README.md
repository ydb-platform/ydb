# Contribution guide

When extending YDB Federated Query list of supported external datasources with new database / storage / whatever,
it's crucial to write integration tests. There's a kind of template for these tests consisting of:

* Test scenario (`CREATE TABLE` / `INSERT` / `SELECT` and so on).
* Test cases parametrizing the scenario.
* Infrastructure code responsible for deploying the external datasource as the dockerized service.

The basic scenario typically should imply the following steps:
1. Deploy datasource and connector services using `docker-compose`.
1. Initialize datasource in any way you like (either with predefined `*.sql` files mounted into container or programmatically from the test app).
1. Prepare `YQL` script to query data from the datasource.
1. Execute script with YDB-based CLI tools: `dqrun` and `kqprun`.
1. Validate output.

## Directory structure

* `common_test_cases` keeps basic test cases that can be used for testing any data source.
* `datasource` contains subfolders (`datasource/clickhouse`, `datasource/postgresql`, etc) with datasource-specific tests scenarios, test cases and `docker-compose.yml` file that is required to set up test environment.
* `join` contains tests checking cross-datasource scenarios.
* `utils` contains building blocks for tests:
    * `utils/clients` stores code performing network IO;
    * `utils/scenario` describes the typical scenarios of the data source usage (e. g. creating table, fullfilling it with test data etc.);
    * `utils/types` describes the external data source's type system.
