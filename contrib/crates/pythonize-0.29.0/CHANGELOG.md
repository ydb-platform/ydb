## 0.29.0 - 2026-05-12

- Update `pyo3` to 0.29.
- Support serializing `i128` and `u128`.

## 0.28.0 - 2026-02-18

- Bump MSRV to 1.83.
- Update `pyo3` to 0.28.
- Support deserializing `dataclass` instances to struct-like Rust types.
- Add `arbitrary_precision` feature

## 0.27.0 - 2025-11-07
- Update to PyO3 0.27

## 0.26.0 - 2025-08-30

### Packaging
- Bump MSRV to 1.74
- Update to PyO3 0.26

### Changed
- `PythonizeTypes`, `PythonizeMappingType` and `PythonizeNamedMappingType` no longer have a lifetime on the trait, instead the `Builder` type is a GAT.

## 0.25.0 - 2025-05-23

### Packaging
- Update to PyO3 0.25

## 0.24.0 - 2025-03-26

### Packaging
- Update to PyO3 0.24

## Removed
- Remove deprecated `depythonize_bound()`

## 0.23.0 - 2024-11-22

### Packaging
- Update to PyO3 0.23

## 0.22.0 - 2024-08-10

### Packaging
- Bump MSRV to 1.63
- Update to PyO3 0.22

### Added
- Support `u128` / `i128` integers.
- Implement `PythonizeListType` for `PyTuple`
- Support deserializing enums from any `PyMapping` instead of just `PyDict`
- Support serializing struct-like types to named mappings using `PythonizeTypes::NamedMap`

### Changed
- `pythonize()` now returns `Bound<'py, PyAny>` instead of `Py<PyAny>`
- `depythonize()` now take `&'a Bound` and is no longer deprecated
- `depythonize_bound()` is now deprecated
- `Depythonizer::from_object()` now takes `&'a Bound` and is no longer deprecated
- `Depythonizer` now contains `&'a Bound` and so has an extra lifetime `'a`

### Removed
- Remove support for PyO3's `gil-refs` feature

### Fixed
- Fix overflow error attempting to depythonize `u64` values greater than `i64::MAX` to types like `serde_json::Value`
- Fix deserializing `set` and `frozenset` into Rust homogeneous containers

## 0.21.1 - 2024-04-02

- Fix compile error when using PyO3 `abi3` feature targeting a minimum version below 3.10

## 0.21.0 - 2024-04-01

- Bump edition to 2021
- Bump MSRV to 1.56
- Update to PyO3 0.21
- Export `PythonizeDefault`

## 0.20.0 - 2023-10-15

- Update to PyO3 0.20

## 0.19.0 - 2023-06-11

- Update to PyO3 0.19

## 0.18.0 - 2023-01-22

- Add LICENSE file to the crate
- Update to PyO3 0.18

## 0.17.0 - 2022-08-24

- Update to PyO3 0.17

## 0.16.0 - 2022-03-06

- Update to PyO3 0.16

## 0.15.0 - 2021-11-12

- Update to PyO3 0.15
- Add `pythonize_custom` for customizing the Python types to serialize to.
- Add support for `depythonize` to handle arbitrary Python sequence and mapping types.

## 0.14.0 - 2021-07-05

- Update to PyO3 0.14

## 0.13.0 - 2020-12-28

- Update to PyO3 0.13

## 0.12.1 - 2020-12-08

- Require `std` feature of `serde`.
- Reduce memory consumption when deserializing sequences.
- Fix deserializing untagged struct enum variants.
- Fix deserializing sequences from Python tuples.

## 0.12.0 - 2020-11-22

- Change release versioning to match `pyo3` major/minor version.
- Implement `depythonizer`

## 0.1.0 - 2020-08-12

- Initial release
