# Changelog

## 1.12.0 04/11/2025

### Changed

- Drop support for Python 3.9
- Add `packb` option `OPT_REPLACE_SURROGATES` to serialize strings that contain
  surrogate code points

### Fixed

- Serialize mutable objects inside a critical section in free threading

## 1.11.0 08/10/2025

### Changed

- Add support for Python 3.14
- Add support for free-threading and subinterpreters
- Add Windows arm64 wheels by [JexinSam](https://github.com/JexinSam) in [#412](/../../pull/412)

## 1.10.0 24/05/2025

### Changed

- Port to PyPy 3.11 and GraalPy 3.11
- Add support for `bytearray` and `memoryview` types by [littledivy](https://github.com/littledivy) in [#374](/../../pull/374)
- Add `packb` and `unpackb` option `OPT_DATETIME_AS_TIMESTAMP_EXT` to serialize
  aware datetime objects to timestamp extension objects and deserialize
  timestamp extension objects to UTC datetime objects, respectively
  [#378](/../../issues/378)

## 1.9.1 28/03/2025

### Changed

- Add musllinux wheels [#366](/../../issues/366)

## 1.9.0 23/03/2025

### Changed

- Add `packb` option `OPT_PASSTHROUGH_ENUM` to enable passthrough of Enum objects by [hinthornw](https://github.com/hinthornw) in [#361](/../../pull/361)
- Add PyInstaller hook [#354](/../../issues/354)
- Update dependencies

## 1.8.0 22/02/2025

### Fixed

- `packb` now rejects dictionary keys with nested dataclasses or pydantic models

### Changed

- Add `packb` option `OPT_PASSTHROUGH_UUID` to enable passthrough of UUID objects [#338](/../../issues/338)
- Update dependencies

## 1.7.0 29/11/2024

### Fixed

- Detect Pydantic 2.10 models [#311](/../../issues/311)
- Fix serialization of dataclasses without `__slots__` and with a field defined
  with a descriptor object as default value, a field defined with `init=False`
  and a default value, or a cached property

### Changed

- Drop support for Python 3.8
- Support `OPT_SORT_KEYS` also for Pydantic models [#312](/../../issues/312)
- Improve deserialization performance

## 1.6.0 18/10/2024

### Fixed

- Deduplicate map keys also when `OPT_NON_STR_KEYS` is set [#279](/../../issues/279)
- Add missing type information for Ext type by [trim21](https://github.com/trim21) in [#285](/../../pull/285)
- Fix type annotation of unpackb first argument

### Changed

- Add support for python 3.13
- Improve test coverage

## 1.5.0 19/04/2024

- Add support for numpy datetime64 and float16 types
- Optimize serialization of dataclasses
- Optimize deserialization of arrays and maps

## 1.4.2 28/01/2024

### Fixed

- Fix crash on termination with Python 3.11 (#223)

### Changed

- Add Linux aarch64 and armv7 wheels (#100, #207)

## 1.4.1 12/11/2023

### Fixed

- Fix performance regression in dict serialization introduced in 1.3.0

## 1.4.0 05/11/2023

### Fixed

- Fix crash in non optimized builds

### Changed

- Add support for MessagePack Extension type
- Add support for numpy 16-bit integers

## 1.3.0 04/10/2023

### Changed

- Drop support for Python 3.7
- Add support for Python 3.12
- Add support for Pydantic 2
- Add `packb` option `OPT_SORT_KEYS` to serialize dictionaries sorted by key
- Update dependencies

## 1.2.6 24/04/2023

### Fixed

- `once_cell` poisoning on parallel initialization by [@Quitlox](https://github.com/Quitlox) in [#153](/../../pull/153)


## 1.2.5 02/02/2023

### Fixed

- aarch64 build on macOS. Took `src/serialize/writer.rs` from upstream orjson. by @ijl
- Fix release on aarch64 to match orjson's upstream.

### Misc

- update dependencies

## 1.2.4 16/11/2022

### Misc

- Fix CI (upgrade maturin, warnings, etc.)

## 1.2.3 - 26/06/2022
### Misc
- Updated dependencies. partially by [@tilman19](/../../pull/101)
- Handle clippy warnings.


## 1.2.2 - 19/4/2022
### Misc
- Update dependencies
## 1.2.1 - 1/3/2022
### Misc
- Release 3.10 wheels
- Update dependencies
## 1.2.0 - 14/2/2022
### Changed
- Extended int passthrough to support u64. by [@pejter](/../../pull/77)
### Misc
- Updated README to include new options. by [@ThomasTNO](/../../pull/70)
- Updated dependencies
- Renamed in `numpy.rs` `from_parent` to `to_children` to fix new lint rules
## 1.1.0 - 8/1/2022
### Added
- Add optional passthrough for tuples. by [@TariqTNO](/../../pull/64)
- Add optional passthrough for ints, that do not fit into an i64. by [@TariqTNO](/../../pull/64)
### Changed
- `opt` parameter can be `None`.
### Misc
- Updated dependencies.
- Dropped 3.6 CI/CD.
- Added macOS universal builds (M1)
## 1.0.3 - 18/12/2021
- Update dependencies
## 1.0.2 - 26/10/2021
- Update dependencies
## 1.0.1 - 13/10/2021
### Fixed
- Decrement refcount for numpy `PyArrayInterface`. by [@ilj](https://github.com/ijl/orjson/commit/4c312a82f5215ff71eed5bd09d28fa004999299b).
- Fix serialization of dataclass inheriting from `abc.ABC` and using `__slots__`. by [@ilj](https://github.com/ijl/orjson/commit/4c312a82f5215ff71eed5bd09d28fa004999299b)
### Changed
- Updated dependencies.
- `find_str_kind` test for 4-byte before latin1. by [@ilj](https://github.com/ijl/orjson/commit/05860e1a2ea3e8f90823d6a59e5fc9929a8692b5)
## 1.0.0 - 31/8/2021
### Changed
-  Aligned to orjson's flags and features of SIMD. Didn't include the stable compilation part as seems unnecessary.
### Misc
- Bumped serde, pyo3.
- Fixed pyproject.toml to work with newest maturin version.
## 0.3.6 - 24/8/2021
### Misc
- Update dependencies.
## 0.3.5 - 5/8/2021
### Fixed
- Fixed clippy warnings for semicolon in macro.
### Misc
- Bumped serde.rs
## 0.3.4 - 23/7/2021
### Fixed
- Fixed `ormsgpack.pyi` support of str as input for `unpackb`.
### Misc
- Fixed Windows CI/CD.
## 0.3.3 - 23/7/2021
### Misc
- Refactored adding objects to the module, creating a `__all__` object similar to the way PyO3 creates. This solves an issue with upgrading to new maturin version.
- Changed < Py3.7 implementation to use automatic range inclusion.
- Added test to validate correct Python method flags are used on declare.
- Changed to use PyO3 configurations instead of our own. PR [#25](/../../pull/25) by [@pejter](https://github.com/pejter).
## 0.3.2 - 13/7/2021
### Fixed
- Fix memory leak serializing `datetime.datetime` with `tzinfo`. (Copied from orjson)
### Changed
- Update dependencies, PyO3 -> 0.14.1.
### Misc
- Setup dependabot.
## 0.3.1 - 19/6/2021
### Changed
- `packb` of maps and sequences is now almost 2x faster as it leverages known size. PR [#18](/../../pull/18) by [@ijl](https://github.com/ijl).
### Misc
- Added `scripts/bench_target.py` and `scripts/profile.sh` for easily benchmarking and profiling. Works only on Linux. PR [#17](/../../pull/17) by [@ijl](https://github.com/ijl).
## 0.3.0 - 13/6/2021
### Added
- `unpackb` now accepts keyword argument `option` with argument `OPT_NON_STR_KEYS`. This option will let ormsgpack
    unpack dictionaries with non-str keys.
    Be aware that this option is considered unsafe and disabled by default in msgpack due to possibility of HashDoS.
- `packb` now is able to pack dictionaries with tuples as keys. `unpackb` is able to unpack such dictionaries. Both requires
    `OPT_NON_STR_KEYS`.
### Misc
- Grouped benchmarks in a pattern that should make more sense.
- Added pydantic docs to `README.md`
- Added graphs and benchmark results.
## 0.2.1 - 12/6/2021
### Fixed
- Depth limit is now enforced for `ormsgpack.unpackb` - function should be safe for use now.
### Removed
- Removed `OPT_SERIALIZE_UUID` from ormsgpack.pyi as it doesn't exist.
### Misc
- Added `scripts/test.sh` for running tests.
- Added benchmarks, modified scripts to match new layout.
## 0.2.0 - 10/6/2021
### Added
- Add support for serializing pydantic's `BaseModel` instances using `ormsgpack.OPT_SERIALIZE_PYDANTIC`.
### Fixed
- `ormsgpack.packb` with `option` argument as `ormsgpack.OPT_NON_STR_KEYS` serializes bytes key into tuple of integers
    instead of using bin type. This also resulted in asymmetrical packb/unpackb.
### Misc
- Added `--no-index` to `pip install ormsgpack` to avoid installing from PyPI on CI.
## 0.1.0 - 9/6/2021

First version, changed orjson to ormsgpack.
