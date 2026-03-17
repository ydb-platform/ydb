# ml_dtypes

[![Unittests](https://github.com/jax-ml/ml_dtypes/actions/workflows/test.yml/badge.svg)](https://github.com/jax-ml/ml_dtypes/actions/workflows/test.yml)
[![Wheel Build](https://github.com/jax-ml/ml_dtypes/actions/workflows/wheels.yml/badge.svg)](https://github.com/jax-ml/ml_dtypes/actions/workflows/wheels.yml)
[![PyPI version](https://badge.fury.io/py/ml_dtypes.svg)](https://badge.fury.io/py/ml_dtypes)

`ml_dtypes` is a stand-alone implementation of several NumPy dtype extensions used in machine learning libraries, including:

- [`bfloat16`](https://en.wikipedia.org/wiki/Bfloat16_floating-point_format):
  an alternative to the standard [`float16`](https://en.wikipedia.org/wiki/Half-precision_floating-point_format) format
- `float8_*`: several experimental 8-bit floating point representations
  including:
  * `float8_e3m4`
  * `float8_e4m3`
  * `float8_e4m3b11fnuz`
  * `float8_e4m3fn`
  * `float8_e4m3fnuz`
  * `float8_e5m2`
  * `float8_e5m2fnuz`
- Microscaling (MX) sub-byte floating point representations including:
  * `float4_e2m1fn`
  * `float6_e2m3fn`
  * `float6_e3m2fn`
- `int2`, `int4`, `uint2` and `uint4`: low precision integer types.

See below for specifications of these number formats.

## Installation

The `ml_dtypes` package is tested with Python versions 3.9-3.12, and can be installed
with the following command:
```
pip install ml_dtypes
```
To test your installation, you can run the following:
```
pip install absl-py pytest
pytest --pyargs ml_dtypes
```
To build from source, clone the repository and run:
```
git submodule init
git submodule update
pip install .
```

## Example Usage

```python
>>> from ml_dtypes import bfloat16
>>> import numpy as np
>>> np.zeros(4, dtype=bfloat16)
array([0, 0, 0, 0], dtype=bfloat16)
```
Importing `ml_dtypes` also registers the data types with numpy, so that they may
be referred to by their string name:

```python
>>> np.dtype('bfloat16')
dtype(bfloat16)
>>> np.dtype('float8_e5m2')
dtype(float8_e5m2)
```

## Specifications of implemented floating point formats

### `bfloat16`

A `bfloat16` number is a single-precision float truncated at 16 bits.

Exponent: 8, Mantissa: 7, exponent bias: 127. IEEE 754, with NaN and inf.

### `float4_e2m1fn`

Exponent: 2, Mantissa: 1, bias: 1.

Extended range: no inf, no NaN.

Microscaling format, 4 bits (encoding: `0bSEEM`) using byte storage (higher 4
bits are unused). NaN representation is undefined.

Possible absolute values: [`0`, `0.5`, `1`, `1.5`, `2`, `3`, `4`, `6`]

### `float6_e2m3fn`

Exponent: 2, Mantissa: 3, bias: 1.

Extended range: no inf, no NaN.

Microscaling format, 6 bits (encoding: `0bSEEMMM`) using byte storage (higher 2
bits are unused). NaN representation is undefined.

Possible values range: [`-7.5`; `7.5`]

### `float6_e3m2fn`

Exponent: 3, Mantissa: 2, bias: 3.

Extended range: no inf, no NaN.

Microscaling format, 4 bits (encoding: `0bSEEEMM`) using byte storage (higher 2
bits are unused). NaN representation is undefined.

Possible values range: [`-28`; `28`]

### `float8_e3m4`

Exponent: 3, Mantissa: 4, bias: 3. IEEE 754, with NaN and inf.

### `float8_e4m3`

Exponent: 4, Mantissa: 3, bias: 7. IEEE 754, with NaN and inf.

### `float8_e4m3b11fnuz`

Exponent: 4, Mantissa: 3, bias: 11.

Extended range: no inf, NaN represented by 0b1000'0000.

### `float8_e4m3fn`

Exponent: 4, Mantissa: 3, bias: 7.

Extended range: no inf, NaN represented by 0bS111'1111.

The `fn` suffix is for consistency with the corresponding LLVM/MLIR type, signaling this type is not consistent with IEEE-754.  The `f` indicates it is finite values only. The `n` indicates it includes NaNs, but only at the outer range.

### `float8_e4m3fnuz`

8-bit floating point with 3 bit mantissa.

An 8-bit floating point type with 1 sign bit, 4 bits exponent and 3 bits mantissa. The suffix `fnuz` is consistent with LLVM/MLIR naming and is derived from the differences to IEEE floating point conventions. `F` is for "finite" (no infinities), `N` for with special NaN encoding, `UZ` for unsigned zero.

This type has the following characteristics:
 * bit encoding: S1E4M3 - `0bSEEEEMMM`
 * exponent bias: 8
 * infinities: Not supported
 * NaNs: Supported with sign bit set to 1, exponent bits and mantissa bits set to all 0s - `0b10000000`
 * denormals when exponent is 0

### `float8_e5m2`

Exponent: 5, Mantissa: 2, bias: 15. IEEE 754, with NaN and inf.

### `float8_e5m2fnuz`

8-bit floating point with 2 bit mantissa.

An 8-bit floating point type with 1 sign bit, 5 bits exponent and 2 bits mantissa. The suffix `fnuz` is consistent with LLVM/MLIR naming and is derived from the differences to IEEE floating point conventions. `F` is for "finite" (no infinities), `N` for with special NaN encoding, `UZ` for unsigned zero.

This type has the following characteristics:
 * bit encoding: S1E5M2 - `0bSEEEEEMM`
 * exponent bias: 16
 * infinities: Not supported
 * NaNs: Supported with sign bit set to 1, exponent bits and mantissa bits set to all 0s - `0b10000000`
 * denormals when exponent is 0

### `float8_e8m0fnu`

[OpenCompute MX](https://www.opencompute.org/documents/ocp-microscaling-formats-mx-v1-0-spec-final-pdf)
scale format E8M0, which has the following properties:
  * Unsigned format
  * 8 exponent bits
  * Exponent range from -127 to 127
  * No zero and infinity
  * Single NaN value (0xFF).

## `int2`, `int4`, `uint2` and `uint4`

2 and 4-bit integer types, where each element is represented unpacked (i.e.,
padded up to a byte in memory).

NumPy does not support types smaller than a single byte: for example, the
distance between adjacent elements in an array (`.strides`) is expressed as
an integer number of bytes. Relaxing this restriction would be a considerable
engineering project. These types therefore use an unpacked representation, where
each element of the array is padded up to a byte in memory. The lower two or four
bits of each byte contain the representation of the number, whereas the remaining
upper bits are ignored.

## Quirks of low-precision Arithmetic

If you're exploring the use of low-precision dtypes in your code, you should be
careful to anticipate when the precision loss might lead to surprising results.
One example is the behavior of aggregations like `sum`; consider this `bfloat16`
summation in NumPy (run with version 1.24.2):

```python
>>> from ml_dtypes import bfloat16
>>> import numpy as np
>>> rng = np.random.default_rng(seed=0)
>>> vals = rng.uniform(size=10000).astype(bfloat16)
>>> vals.sum()
256
```
The true sum should be close to 5000, but numpy returns exactly 256: this is
because `bfloat16` does not have the precision to increment `256` by values less than
`1`:

```python
>>> bfloat16(256) + bfloat16(1)
256
```
After 256, the next representable value in bfloat16 is 258:

```python
>>> np.nextafter(bfloat16(256), bfloat16(np.inf))
258
```
For better results you can specify that the accumulation should happen in a
higher-precision type like `float32`:

```python
>>> vals.sum(dtype='float32').astype(bfloat16)
4992
```
In contrast to NumPy, projects like [JAX](http://jax.readthedocs.io/) which support
low-precision arithmetic more natively will often do these kinds of higher-precision
accumulations automatically:

```python
>>> import jax.numpy as jnp
>>> jnp.array(vals).sum()
Array(4992, dtype=bfloat16)
```

## License

*This is not an officially supported Google product.*

The `ml_dtypes` source code is licensed under the Apache 2.0 license
(see [LICENSE](LICENSE)). Pre-compiled wheels are built with the
[EIGEN](https://eigen.tuxfamily.org/) project, which is released under the
MPL 2.0 license (see [LICENSE.eigen](LICENSE.eigen)).
