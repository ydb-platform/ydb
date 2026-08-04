PocketFFT for C++
=================

This is a heavily modified implementation of FFTPack [1,2], with the following
advantages:

- Strictly C++11 compliant
- More accurate twiddle factor computation
- Worst case complexity for transform sizes with large prime factors is
  `N*log(N)`, because Bluestein's algorithm [3] is used for these cases.
- Supports multidimensional arrays and selection of the axes to be transformed.
- Supports `float`, `double`, and `long double` types.
- Supports fully complex and half-complex (i.e. complex-to-real and
  real-to-complex) FFTs. For half-complex transforms, several conventions for
  representing the complex-valued side are supported (reduced-size complex
  array, FFTPACK-style half-complex format and Hartley transform).
- Supports discrete cosine and sine transforms (Types I-IV)
- Makes use of CPU vector instructions when performing 2D and higher-dimensional
  transforms, if they are available.
- Has a small internal cache for transform plans, which speeds up repeated
  transforms of the same length (most significant for 1D transforms).
- Has optional multi-threading support for multidimensional transforms


License
-------

3-clause BSD (see LICENSE.md)


Some code details
-----------------

Twiddle factor computation:

- making use of symmetries to reduce number of sin/cos evaluations
- all angles are reduced to the range `[0; pi/4]` for higher accuracy
- if `n` sin/cos pairs are required, the trogonometric functions are only called
  `2*sqrt(n)` times; the remaining values are obtained by evaluating the
  angle addition theorems in a numerically accurate way.

Efficient codelets are available for the factors:

- 2, 3, 4, 5, 7, 8, 11 for complex-valued FFTs
- 2, 3, 4, 5 for real-valued FFTs

Larger prime factors are handled by somewhat less efficient, generic routines.

For lengths with very large prime factors, Bluestein's algorithm is used, and
instead of an FFT of length `n`, a convolution of length `n2 >= 2*n-1`
is performed, where `n2` is chosen to be highly composite.


[1] Swarztrauber, P. 1982, Vectorizing the Fast Fourier Transforms
    (New York: Academic Press), 51

[2] https://www.netlib.org/fftpack/

[3] https://en.wikipedia.org/wiki/Chirp_Z-transform


Configuration options
=====================

Since this is a header-only library, it can only be configured via preprocessor
macros.

POCKETFFT_CACHE_SIZE:\
if 0, disable all caching of FFT plans, else use an LRU cache with the
requested size. If undefined, assume a cache size of 0.\
NOTE: caching is disabled by default because its benefits are only really
noticeable for short 1D transforms. When using caching with transforms that
have very large axis lengths, it may use up a lot of memory, so only switch this
on if you know you really need it!
Default: undefined

POCKETFFT_NO_VECTORS:\
if defined, disable all support for CPU vector instructions.\
Default: undefined

POCKETFFT_NO_MULTITHREADING:\
if defined, multi-threading will be disabled.\
Default: undefined


Programming interface
=====================

All symbols are encapsulated in the namespace `pocketfft`.

Arguments
---------
 - `shape[_*]` contains the number of array entries along each axis.
   For `c2c` and `r2r` transforms, `shape` is identical for input and output
   arrays. For `r2c` transforms the shape of the input array must be specified,
   while for `c2r` transforms the shape of the *output* array must be given.

 - `stride_*` describes array strides, i.e. the memory distance (in bytes)
   between two neighboring array entries along an axis.

 - `axes` is a vector of nonnegative integers, describing the axes along
   which a transform is to be carried out. The order of axes usually does not
   matter, except for `r2c` and `c2r` transforms, where the last entry of
   `axes` is treated specially.

 - `forward` describes the direction of a transform. Generally a forward
   transform has a minus sign in the complex exponent, while the backward
   transform has a positive one. Instead if `true`/`false`, the symbolic
   constants `FORWARD`/`BACKWARD` can be used.
   NOTE: Unlike many other libraries, pocketfft also allows a `forward` argument
   in `r2c` and `c2r` transforms, instead of having hard-wired forward `r2c` and
   backward `c2r` transforms. Calling `r2c` with `forward=false`, for
   example, performs a transform from purely real data in the frequency domain
   to Hermitian data in the position domain.
   If you want the "traditional" behavior, call `r2c` with `forward=true` and
   `c2r` with `forward=false`.

 - `fct` is a floating-point value which is used to scale the result of a
   transform. `pocketfft`'s transforms are not normalized, so if normalization
   is required, an appropriate scaling factor has to be specified.

 - `data_in` and `data_out` are pointers to the first element of the input
   and output data arrays.

 - `nthreads` is a nonnegative integer specifying the number of threads to use
   for the operation. A value of 0 means that the number of logical CPU cores
   will be used.
   This value is only a recommendation. If `pocketfft` is compiled without
   multi-threading support, it will be silently ignored. For one-dimensional
   transforms, multi-threading is disabled as well.

General constraints on arguments
--------------------------------
 - `shape[_*]`, `stride_in` and `stride_out` must have the same `size()`
   and must not be empty.
 - Entries in `shape[_*]` must be >=1.
 - If `data_in==data_out`, `stride_in` and `stride_out` must have identical
   content. These in-place transforms are fine for `c2c` and `r2r`, but not for
   `r2c/c2r`.
 - Axes are numbered from 0 to `shape.size()-1`, inclusively.
 - Strides are measured in bytes, to allow maximum flexibility. Negative strides
   are fine. Strides that lead to multiple accesses of the same memory address
   are not allowed.
 - All memory addresses resulting from a combination of data pointers and
   strides must have sufficient alignment. On x86 CPUs, badly aligned adresses
   will only lead to slower execution, but on some other hardware, misaligned
   memory accesses will cause a crash.
 - The same axis must not be specified more than once in an `axes` argument.
 - For `r2c` and `c2r` transforms: the length of the complex array along `axis`
   (or the last entry in `axes`) is assumed to be `s/2 + 1`, where `s` is the
   length of the corresponding axis of the real array.

Detailed public interface
-------------------------

```
using shape_t = std::vector<std::size_t>;
using stride_t = std::vector<std::ptrdiff_t>;

constexpr bool FORWARD  = true,
               BACKWARD = false;

template<typename T> void c2c(const shape_t &shape, const stride_t &stride_in,
  const stride_t &stride_out, const shape_t &axes, bool forward,
  const complex<T> *data_in, complex<T> *data_out, T fct,
  size_t nthreads=1)

template<typename T> void r2c(const shape_t &shape_in,
  const stride_t &stride_in, const stride_t &stride_out, size_t axis,
  bool forward, const T *data_in, complex<T> *data_out, T fct,
  size_t nthreads=1)

/* This function first carries out an r2c transform along the last axis in axes,
   storing the result in data_out. Then, an in-place c2c transform
   is carried out in data_out along all other axes. */
template<typename T> void r2c(const shape_t &shape_in,
  const stride_t &stride_in, const stride_t &stride_out, const shape_t &axes,
  bool forward, const T *data_in, complex<T> *data_out, T fct,
  size_t nthreads=1)

template<typename T> void c2r(const shape_t &shape_out,
  const stride_t &stride_in, const stride_t &stride_out, size_t axis,
  bool forward, const complex<T> *data_in, T *data_out, T fct,
  size_t nthreads=1)

/* This function first carries out a c2c transform along all axes except the
   last one, storing the result into a temporary array. Then, a c2r transform
   is carried out along the last axis, storing the result in data_out. */
template<typename T> void c2r(const shape_t &shape_out,
  const stride_t &stride_in, const stride_t &stride_out, const shape_t &axes,
  bool forward, const complex<T> *data_in, T *data_out, T fct,
  size_t nthreads=1)

/* This function carries out a FFTPACK-style real-to-halfcomplex or
   halfcomplex-to-real transform (depending on the parameter `real2hermitian`)
   on all specified axes in the given order.
   NOTE: interpreting the result of this function can be complicated when
   transforming more than one axis! */
template<typename T> void r2r_fftpack(const shape_t &shape,
  const stride_t &stride_in, const stride_t &stride_out, const shape_t &axes,
  bool real2hermitian, bool forward, const T *data_in, T *data_out, T fct,
  size_t nthreads=1)

/* For every requested axis, this function carries out a forward Fourier
   transform, and the real and imaginary parts of the result are added before
   the next axis is processed.
   This is analogous to FFTW's implementation of the Hartley transform. */
template<typename T> void r2r_separable_hartley(const shape_t &shape,
  const stride_t &stride_in, const stride_t &stride_out, const shape_t &axes,
  const T *data_in, T *data_out, T fct, size_t nthreads=1);

/* This function carries out a full Fourier transform over the requested axes,
   and the sum of real and imaginary parts of the result is stored in the output
   array. For a single transformed axis, this is identical to
   `r2r_separable_hartley`, but when transforming multiple axes, the results
   are different.

   NOTE: This function allocates temporary working space with a size
   comparable to the input array. */
template<typename T> void r2r_genuine_hartley(const shape_t &shape,
  const stride_t &stride_in, const stride_t &stride_out, const shape_t &axes,
  const T *data_in, T *data_out, T fct, size_t nthreads=1);

/* if ortho==true, the transform is made orthogonal by these additional steps
   in every 1D sub-transform:
   Type 1 : multiply first and last input value by sqrt(2)
            divide first and last output value by sqrt(2)
   Type 2 : divide first output value by sqrt(2)
   Type 3 : multiply first input value by sqrt(2)
   Type 4 : nothing */
template<typename T> void dct(const shape_t &shape,
  const stride_t &stride_in, const stride_t &stride_out, const shape_t &axes,
  int type, const T *data_in, T *data_out, T fct, bool ortho,
  size_t nthreads=1);

/* if ortho==true, the transform is made orthogonal by these additional steps
   in every 1D sub-transform:
   Type 1 : nothing
   Type 2 : divide last output value by sqrt(2)
   Type 3 : multiply last input value by sqrt(2)
   Type 4 : nothing */
template<typename T> void dst(const shape_t &shape,
  const stride_t &stride_in, const stride_t &stride_out, const shape_t &axes,
  int type, const T *data_in, T *data_out, T fct, bool ortho,
  size_t nthreads=1);
```
