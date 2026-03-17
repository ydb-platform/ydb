# Snowflake Connector for Python

[![Build and Test](https://github.com/snowflakedb/snowflake-connector-python/actions/workflows/build_test.yml/badge.svg)](https://github.com/snowflakedb/snowflake-connector-python/actions/workflows/build_test.yml)
[![codecov](https://codecov.io/gh/snowflakedb/snowflake-connector-python/branch/main/graph/badge.svg?token=MVKSNtnLr0)](https://codecov.io/gh/snowflakedb/snowflake-connector-python)
[![PyPi](https://img.shields.io/pypi/v/snowflake-connector-python.svg)](https://pypi.python.org/pypi/snowflake-connector-python/)
[![License Apache-2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Codestyle Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This package includes the Snowflake Connector for Python, which conforms to the [Python DB API 2.0](https://www.python.org/dev/peps/pep-0249/) specification.

The Snowflake Connector for Python provides an interface for developing Python
applications that can connect to Snowflake and perform all standard operations. It
provides a programming alternative to developing applications in Java or C/C++
using the Snowflake JDBC or ODBC drivers.

The connector has **no** dependencies on JDBC or ODBC.
It can be installed using ``pip`` on Linux, Mac OSX, and Windows platforms
where Python 3.7.0 (or higher) is installed.

Snowflake Documentation is available at:
https://docs.snowflake.com/

Feel free to file an issue or submit a PR here for general cases. For official support, contact Snowflake support at:
https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge

## How to build

### Locally

Install Python 3.7.0 or higher. Clone the Snowflake Connector for Python repository, then run the following commands
to create a wheel package using PEP-517 build:

```shell
git clone git@github.com:snowflakedb/snowflake-connector-python.git
cd snowflake-connector-python
python -m pip install -U pip setuptools wheel build
python -m build --wheel .
```

Find the `snowflake_connector_python*.whl` package in the `./dist` directory.

### In Docker
Or use our Dockerized build script `ci/build_docker.sh` and find the built wheel files in `dist/repaired_wheels`.

Note: `ci/build_docker.sh` can be used to compile only certain versions, like this: `ci/build_docker.sh "3.7 3.8"`

## Code hygiene and other utilities
These tools are integrated into `tox` to allow us to easily set them up universally on any computer.

* **fix_lint**: Runs `pre-commit` to check for a bunch of lint issues. This can be installed to run upon each
  time a commit is created locally, keep an eye out for the hint that this environment prints upon succeeding.
* **coverage**: Runs `coverage.py` to combine generated coverage data files. Useful when multiple categories were run
  and we would like to have an overall coverage data file created for them.
* **flake8**: (Deprecated) Similar to `fix_lint`, but only runs `flake8` checks.
