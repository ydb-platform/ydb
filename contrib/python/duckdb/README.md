<div align="center">
  <picture>
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/duckdb/duckdb/refs/heads/main/logo/DuckDB_Logo-horizontal.svg">
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/duckdb/duckdb/refs/heads/main/logo/DuckDB_Logo-horizontal-dark-mode.svg">
    <img alt="DuckDB logo" src="https://raw.githubusercontent.com/duckdb/duckdb/refs/heads/main/logo/DuckDB_Logo-horizontal.svg" height="100">
  </picture>
</div>
<br />
<p align="center">
  <a href="https://discord.gg/tcvwpjfnZx"><img src="https://shields.io/discord/909674491309850675" alt="Discord" /></a>
  <a href="https://pypi.org/project/duckdb/"><img src="https://img.shields.io/pypi/v/duckdb.svg" alt="PyPI Latest Release"/></a>
</p>
<br />
<p align="center">
  <a href="https://duckdb.org">DuckDB.org</a>
  |
  <a href="https://duckdb.org/docs/stable/guides/python/install">User Guide (Python)</a>
  -
  <a href="https://duckdb.org/docs/stable/clients/python/overview">API Docs (Python)</a>
</p>

# DuckDB: A Fast, In-Process, Portable, Open Source, Analytical Database System

* **Simple**: DuckDB is easy to install and deploy. It has zero external dependencies and runs in-process in its host application or as a single binary.
* **Portable**: DuckDB runs on Linux, macOS, Windows, Android, iOS and all popular hardware architectures. It has idiomatic client APIs for major programming languages.
* **Feature-rich**: DuckDB offers a rich SQL dialect. It can read and write file formats such as CSV, Parquet, and JSON, to and from the local file system and remote endpoints such as S3 buckets.
* **Fast**: DuckDB runs analytical queries at blazing speed thanks to its columnar engine, which supports parallel execution and can process larger-than-memory workloads.
* **Extensible**: DuckDB is extensible by third-party features such as new data types, functions, file formats and new SQL syntax. User contributions are available as community extensions.
* **Free**: DuckDB and its core extensions are open-source under the permissive MIT License. The intellectual property of the project is held by the DuckDB Foundation.

## Installation

Install the latest release of DuckDB directly from [PyPI](https://pypi.org/project/duckdb/):

```bash
pip install duckdb
```

Install with all optional dependencies:

```bash
pip install 'duckdb[all]'
```

## Contributing

See the [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on how to set up a development environment.
