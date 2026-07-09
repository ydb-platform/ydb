# YDB Tests

Follow the testing style of the module you are modifying. For new C++ unit tests,
prefer **UNITTEST** over **GTEST**.

## C++ unit tests

- Place tests in a `ut/` subdirectory next to the code under test.
- Declare the test module in `ya.make` with `UNITTEST_FOR(...)` (or `UNITTEST()`
  for standalone test targets).
- Register test sources in `TEST_SRCS()`.
- Write tests with `Y_UNIT_TEST_SUITE` / `Y_UNIT_TEST` from
  `library/cpp/testing/unittest/registar.h`.

Use `GTEST()` only when the surrounding code already does (e.g. some
`ydb/public/sdk/cpp/tests/` targets). Do not introduce new GTest-based tests
where UNITTEST is used in the same area.

## Running tests

```bash
./ya make --build relwithdebinfo -tA <folder>
./ya make --build relwithdebinfo -tA <folder> -F *test-filter*
```

See [`AGENTS.md`](AGENTS.md) for more build and test conventions.
