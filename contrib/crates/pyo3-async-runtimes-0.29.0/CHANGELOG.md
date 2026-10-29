# Changelog

All notable changes to this project will be documented in this file. For help with updating to new
PyO3 versions, please see the [migration guide](https://pyo3.rs/latest/migration.html).

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

To see unreleased changes, please see the CHANGELOG on the main branch.

<!-- towncrier release notes start -->

## [Unreleased]

## [0.29.0] - 2026-06-12

- chore: bump pyo3 version to 0.29. [#85](https://github.com/PyO3/pyo3-async-runtimes/pull/85)

## [0.28.0] - 2026-02-03

- Bump to pyo3 0.28. [#76](https://github.com/PyO3/pyo3-async-runtimes/pull/76)
- Check for awaitable instead of coroutine in stream glue [#73](https://github.com/PyO3/pyo3-async-runtimes/pull/73)
- Bump MSRV to 1.83.
- Fix handling of full buffer in `into_stream` functions

## [0.27.0] - 2025-10-20

- Avoid attaching to the runtime when cloning TaskLocals by using std::sync::Arc. [#62](https://github.com/PyO3/pyo3-async-runtimes/pull/62)
- **Breaking**: Finalize the future without holding GIL inside async-std/tokio runtime.
  Trait `Runtime` now requires `spawn_blocking` function,
  `future_into_py` functions now require future return type to be `Send`.
  [#60](https://github.com/PyO3/pyo3-async-runtimes/pull/60)
- Change pyo3 `downcast` calls to `cast` calls [#65](https://github.com/PyO3/pyo3-async-runtimes/pull/65)
- Use `pyo3::intern!` for method calls and `getattr` calls [#66](https://github.com/PyO3/pyo3-async-runtimes/pull/66)
- Fix missing LICENSE file in macros crate by @musicinmybrain in https://github.com/PyO3/pyo3-async-runtimes/pull/63
- Bump to pyo3 0.27. [#68](https://github.com/PyO3/pyo3-async-runtimes/pull/68)

## [0.26.0] - 2025-09-02

- Bump to pyo3 0.26. [#54](https://github.com/PyO3/pyo3-async-runtimes/pull/54)

## [0.25.0] - 2025-05-14

- Bump to pyo3 0.25. [#41](https://github.com/PyO3/pyo3-async-runtimes/pull/41)

## [0.24.0] - 2025-03-11

- Bump to pyo3 0.24. [#34](https://github.com/PyO3/pyo3-async-runtimes/pull/34)

## [0.23.0] - 2024-11-22

- Bump minimum version of `pyo3` dependency to 0.23. [#21](https://github.com/PyO3/pyo3-async-runtimes/pull/21)

## [0.22.0] - 2024-10-28

- Move from [the `davidhewitt/pyo3-asyncio` fork](https://github.com/davidhewitt/pyo3-asyncio) (had been published as `pyo3-asyncio-0.21`) to `pyo3-async-runtimes`.
- Bump minimum version of `syn` dependency to 2. [#12](https://github.com/PyO3/pyo3-async-runtimes/pull/12)

## Older versions

Previous versions were published from [`pyo3-asyncio`](https://github.com/awestlake87/pyo3-asyncio). Consult that library for older changes.

[Unreleased]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.29.0...HEAD
[0.29.0]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.28.0...v0.29.0
[0.28.0]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.27.0...v0.28.0
[0.27.0]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.26.0...v0.27.0
[0.26.0]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.25.0...v0.26.0
[0.25.0]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.24.0...v0.25.0
[0.24.0]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.23.0...v0.24.0
[0.23.0]: https://github.com/PyO3/pyo3-async-runtimes/compare/v0.22.0...v0.23.0
[0.22.0]: https://github.com/PyO3/pyo3-async-runtimes/tree/0.22.0
