# YDB Tests

Follow the testing style of the module you are modifying. For new C++ unit tests,
prefer **UNITTEST** over **GTEST**.

## C++ unit tests

- Place tests in a `ut/` subdirectory next to the code under test.
- Declare the test module in `ya.make` with `UNITTEST_FOR(...)` (or `UNITTEST()`
  for standalone test targets).
- Register C++ test sources in `SRCS()`.
- Write tests with `Y_UNIT_TEST_SUITE` / `Y_UNIT_TEST` from
  `library/cpp/testing/unittest/registar.h`.

Use `GTEST()` only when the surrounding code already does (e.g. some
`ydb/public/sdk/cpp/tests/` targets). Do not introduce new GTest-based tests
where UNITTEST is used in the same area.

## Python tests

- Use the `unittest` framework (`unittest.IsolatedAsyncioTestCase` for async code).
- Register Python test files in `TEST_SRCS()` in the module's `ya.make`.
- Third-party packages come from `contrib/python`.
- See [`ydb/tools/mnc/AGENTS.md`](../ydb/tools/mnc/AGENTS.md) for MNC-specific
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
  [`ydb/tests/compatibility/README.md`](../ydb/tests/compatibility/README.md).

## Running tests

```bash
./ya make --build relwithdebinfo -tA <folder>
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*
```

CLI tests (`ya make -tA`) run only on Linux.

Build rules: [`AGENTS.md`](AGENTS.md).
