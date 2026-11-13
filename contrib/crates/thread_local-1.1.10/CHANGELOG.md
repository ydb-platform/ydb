# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.10](https://github.com/Amanieu/thread_local-rs/compare/v1.1.9...v1.1.10) - 2026-07-10

### Fixed

- Fix undefined behavior when `get_or` or `get_or_try` reentrantly initializes the same `ThreadLocal` (#97).
- Fix an integer underflow in iterator `size_hint` implementations (#89).
- Fix compilation with the `nightly` feature enabled (#96).

### Changed

- `ThreadLocal::get` no longer allocates a thread ID or registers a thread-local destructor when no value exists for the current thread (#84).

## [1.1.9](https://github.com/Amanieu/thread_local-rs/compare/v1.1.8...v1.1.9) - 2025-06-12

### Other

- Add release-plz for automated releases
- Remove once_cell dependency
- Bump MSRV to 1.61
