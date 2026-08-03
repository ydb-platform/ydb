# C++ test framework preference

Scope: the **test module** — the `ut/` directory (its `ya.make` and sources).

- If that `ut/` **already has** C++ unit tests, use the **same** framework
  (`UNITTEST` or `GTEST`) as the existing tests there.
- If that `ut/` has **no** C++ unit tests yet, prefer **GTEST**.

Test types and how to run them: [`TESTS.md`](TESTS.md).
