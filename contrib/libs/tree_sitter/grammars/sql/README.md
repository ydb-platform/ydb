# tree-sitter-sql

[![Build/test](https://github.com/derekstride/tree-sitter-sql/actions/workflows/ci.yml/badge.svg)](https://github.com/derekstride/tree-sitter-sql/actions/workflows/ci.yml)
[![GitHub Pages](https://github.com/DerekStride/tree-sitter-sql/actions/workflows/gh-pages.yml/badge.svg)](https://github.com/DerekStride/tree-sitter-sql/actions/workflows/gh-pages.yml)
[![npm package version](https://img.shields.io/npm/v/%40derekstride/tree-sitter-sql?logo=npm&color=brightgreen)](https://www.npmjs.com/package/@derekstride/tree-sitter-sql)


A general/permissive SQL grammar for [tree-sitter](https://github.com/tree-sitter/tree-sitter).

## Installation

**We don't commit the generated parser files to the `main` branch.** Instead, you can find them on the
[gh-pages](https://github.com/DerekStride/tree-sitter-sql/tree/gh-pages) branch. We're open to feedback & encourage you
to [open an issue](https://github.com/DerekStride/tree-sitter-sql/issues/new) to discuss any problems.

They are also hosted on the [GitHub pages site](https://derek.stride.host/tree-sitter-sql/) and available for download
here:
[github://derekstride/tree-sitter-sql/gh-pages.tar.gz](https://github.com/DerekStride/tree-sitter-sql/archive/refs/heads/gh-pages.tar.gz).

*Plugin maintainers ensure to specify the `HEAD` (or a specific revision) of the `gh-pages` branch when integrating
with this project.*

### Step 1: Download the parser files

**Using `git`**
```bash
git clone https://github.com/DerekStride/tree-sitter-sql.git
cd tree-sitter-sql
git checkout gh-pages
```

**Using `curl`**
```bash
curl -LO https://github.com/DerekStride/tree-sitter-sql/archive/refs/heads/gh-pages.tar.gz
tar -xzf gh-pages.tar.gz
cd tree-sitter-sql-gh-pages
```

### Step 2: Compile the Parser

Tree-sitter parsers need to be compiled as a shared-object / dynamic-library, you can enable this by passing the
`-shared` & `-fPIC` flags to your compiler.

```bash
cc -shared -fPIC -I./src src/parser.c src/scanner.c -o sql.so
```

### Using [cargo](https://crates.io/crates/tree-sitter-sequel)

```bash
cargo add tree-sitter-sequel
```

### Using [npm](https://www.npmjs.com/package/@derekstride/tree-sitter-sql)

```bash
npm i @derekstride/tree-sitter-sql
```

### Using [pip](https://pypi.org/project/tree-sitter-sql/0.3.5/)

```bash
pip install tree-sitter-sql
```

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md) for documentation on how to set up the project for development.

## Features

For a complete list of features see the the [tests](test/corpus)

## References

* [Wikipedia#SQL_syntax](https://en.wikipedia.org/wiki/SQL_syntax) - I consulted wikipedia for naming conventions,
  though I may not have been strict early on in the prototyping.
* [Phoenix Language Reference](https://forcedotcom.github.io/phoenix/index.html) - A reference diagram.
* [SQLite's railroad diagram for expr](https://www.sqlite.org/lang_expr.html) - Another reference diagram.
* [Postgresql syntax documentation](https://www.postgresql.org/docs/current/sql-commands.html)
* [Mariadb syntax documentation](https://mariadb.com/kb/en/sql-statements-structure/)

### Other projects

* https://github.com/m-novikov/tree-sitter-sql
* https://github.com/tjdevries/tree-sitter-sql
* https://github.com/dhcmrlchtdj/tree-sitter-sqlite
