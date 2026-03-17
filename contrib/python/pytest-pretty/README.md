# pytest-pretty

[![CI](https://github.com/samuelcolvin/pytest-pretty/actions/workflows/ci.yml/badge.svg)](https://github.com/samuelcolvin/pytest-pretty/actions?query=event%3Apush+branch%3Amain+workflow%3ACI)
[![pypi](https://img.shields.io/pypi/v/pytest-pretty.svg)](https://pypi.python.org/pypi/pytest-pretty)
[![versions](https://img.shields.io/pypi/pyversions/pytest-pretty.svg)](https://github.com/samuelcolvin/pytest-pretty)
[![license](https://img.shields.io/github/license/samuelcolvin/pytest-pretty.svg)](https://github.com/samuelcolvin/pytest-pretty/blob/main/LICENSE)

Opinionated pytest plugin to make output slightly easier to read and errors easy to find and fix.

pytest-pretty's only dependencies are [rich](https://pypi.org/project/rich/) and pytest itself.

### Realtime error summary

One-line info on which test has failed while tests are running:

![Realtime Error Summary](./screenshots/realtime-error-summary.png)

### Table of failures

A rich table of failures with both test line number and error line number:

![Table of Failures](./screenshots/table-of-failures.png)

This is extremely useful for navigating to failed tests without having to scroll through the entire test output.

### Prettier Summary of a Test Run

Including time taken for the test run:

![Test Run Summary](./screenshots/test-run-summary.png)

## Installation

```sh
pip install -U pytest-pretty
```

## Usage with GitHub Actions

If you're using pytest-pretty (or indeed, just pytest) with GitHub Actions, it's worth adding the following to the top of your workflow `.yml` file:

```yaml
env:
  COLUMNS: 120
```

This will mean the pytest output is wider and easier to use, more importantly, it'll make the error summary table printed by pytest-pretty much easier to read, see [this](https://github.com/Textualize/rich/issues/2769) discussion for more details.

## `pytester_pretty` fixture

The `pytest_pretty` provides `pytester_pretty` fixture that work with modified version of output. It is designed to drop in places replacement of `pytester` fixture and uses it internally.

So to use them it is required to set `pytest_plugins = "pytester"` as mentioned in pytest documentation <https://docs.pytest.org/en/latest/reference/reference.html#pytester>
