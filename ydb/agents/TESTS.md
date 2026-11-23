# YDB Tests

Follow the testing style of the module you are modifying.

## C++ unit tests

- Place tests in a `ut/` subdirectory next to the code under test.
- Name test source files `*_ut.cpp`.
- Declare the test module in `ya.make` with `GTEST()`, or
  `UNITTEST()` / `UNITTEST_FOR(...)` when matching existing tests in that `ut/`.
- Register C++ test sources in `SRCS()`.

## Python tests

- Use the `unittest` framework (`unittest.IsolatedAsyncioTestCase` for async code).
- Register Python test files in `TEST_SRCS()` in the module's `ya.make`.
- Third-party packages come from `contrib/python`.
- See [`ydb/tools/mnc/AGENTS.md`](../tools/mnc/AGENTS.md) for MNC-specific
  Python test conventions.

## Functional tests

End-to-end tests that run against a YDB cluster live under `ydb/tests/functional/`.

- Declare targets with `PY3TEST()` in `ya.make`.
- List test files in `TEST_SRCS()` (paths are relative to the repository root).
- Follow existing harness patterns in the same area (`INCLUDE(...harness_dep.inc)`,
  `ENV(...)`, suite `.inc` files).

## Compatibility tests

Upgrade/downgrade tests live under `ydb/tests/compatibility/`.

- Place each test module in the folder matching the feature area.
- Use existing fixtures and follow the layout described in
  [`ydb/tests/compatibility/README.md`](../tests/compatibility/README.md).

## Running tests

```bash
./ya make --build relwithdebinfo -tA <folder>               # all tests
./ya make --build relwithdebinfo -tA <folder> -F <filter>   # matching tests
./ya make -t <folder> -L                                    # list full test names
```

CLI tests (`ya make -tA`) run only on Linux.

`<filter>` is a full test name, its suffix, or a `*` glob (`*part*`):

- C++ (`UNITTEST`, `GTEST`): `TMySuite::MyTest`; `TMySuite` runs the whole suite.
- Python (pytest, `unittest`): `test_foo.py::TestClass::test_bar[param]`, no class
  part for module-level tests; `test_foo.py` runs the whole module,
  `test_foo.py::TestClass::*` the whole class. `test_foo.py::TestClass` without
  `::*` matches nothing and still reports `Ok`.

Build rules: [`GUIDE.md`](GUIDE.md).
