## `yt/yt/library/numeric` — Numeric Utilities

A collection of mostly header-only numeric algorithms and data structures. No single entry point — include the relevant header directly. Only one translation unit (`piecewise_linear_function.cpp`) contributes real code; everything else is templates.

**Components:**

**`binary_search.h`** — Predicate-based binary search over integer and floating-point domains.
- `IntegerLowerBound(lo, hi, pred)` / `IntegerInverseLowerBound` — find the boundary integer where a monotone predicate flips.
- `FloatingPointLowerBound(lo, hi, pred)` / `FloatingPointInverseLowerBound` — same for `double`, over the IEEE 754 bit pattern (≤70 predicate calls: 64 bisection + 6 reserved for corner cases). Prefer these over hand-rolled bisection loops. Handles infinities; NaN inputs are undefined.

**`algorithm_helpers.h`** — Iterator-based search and misc algorithms.
- `LinearSearch(begin, end, pred)` — companion to `BinarySearch`, for short ranges or non-sorted predicates.
- `BinarySearch(begin, end, pred)` / `ExponentialSearch` — predicate-based iterator search (returns first iterator where `!pred`).
- `LowerBound` / `UpperBound` / `ExpLowerBound` / `ExpUpperBound` — value-based wrappers.
- `Intersects(first1, last1, first2, last2)` — whether two sorted ranges share any element, without materializing the intersection.
- `PartialShuffle(begin, end, last)` — Fisher-Yates to pick a random K-subset in-place.
- `MinMaxBy(a, b, getKey)` — returns `{min, max}` by a key function.

**`piecewise_linear_function.h`** — Piecewise linear functions over `double` with a generic value type. Used heavily by `vector_hdrf`.
- `TPiecewiseLinearFunction<TValue>` — the main class. Left-continuous; discontinuities are represented as vertical segments. Sample with `ValueAt`, `LeftLimitAt`, `RightLimitAt`, or `LeftRightLimitAt`. Segment access via `LeftSegmentAt` / `RightSegmentAt` / `SegmentAt` (with optional `segmentIndex` out-param).
- Factories: `TPiecewiseLinearFunction::Create(sample, left, right, criticalPoints)`, `::Linear`, `::Constant`.
- `TPiecewiseLinearFunctionBuilder<TValue>` — incremental construction via `AddPoint` / `PushSegment`, then `Finish()`.
- Algebra: `operator+`, `Sum`, `Compose` (left composition with a non-decreasing function), `PointwiseMin` (free functions). Domain manipulation: `Transpose`/`Inplace` (invert), `Narrow`/`Inplace`, `Extend`/`Inplace` (and `ExtendRight`), `ScaleArgument`, `Shift`. Trimming of sentinel discontinuities: `Trim` / `TrimLeft` / `TrimRight` with `Inplace` variants and rvalue overloads for chaining.
- Queries: `IsContinuous`, `IsNondecreasing`, `IsDefinedAt`, `IsTrimmed` (+ `IsTrimmedLeft`/`Right`), `LeftFunctionBound`/`Value`, `RightFunctionBound`/`Value`, `Segments()`.
- `TLeftToRightTraverser` — amortized O(1) sequential access; use when sampling the function at many monotonically increasing points. Obtain via `GetLeftToRightTraverser(segmentIndex)`. Non-monotonic use is undefined.
- Free helper: `ClearAndSortCriticalPoints(vec, leftBound, rightBound)` — dedups, drops out-of-range points, and sorts. The implementation (in the `.cpp`) detects "almost-sorted" input with a bounded number of pivots and runs a k-way merge instead of `std::sort`.

**`double_array.h`** — Fixed-size real vectors (mathematical vectors, not `std::vector`).
- `TDoubleArrayBase<DimCnt, TDerived>` — CRTP base for custom fixed-dimension vector types; subclass to add domain-specific factory methods and `operator()`.
- `TDoubleArray<DimCnt>` — final general-purpose vector; supports iteration, `operator[]`, arithmetic (`+`, `-`, unary `-`, `+=`, `-=`, `*` / `/` by scalar), `Div` with explicit `0/0` and `x/0` fallbacks, `operator==`, `Dominates` (component-wise `>=`), `MinComponent` / `MaxComponent`, and static helpers `All`, `Apply`, `ForEach`, `FromDouble`, ... (on the CRTP base).
- Concept: `IsDoubleArray<T>` constrains the free-function overloads above.

**`double_array_format.h`** — `ostream` overload for `TDoubleArray` (in `NDetail` namespace) for nicer test output. Include when you need `operator<<` for `std::ostream`.

**`fixed_point_number.h`** — `TFixedPointNumber<Underlying, DecimalPrecision>`.
- Stores an X.YYY number as an integer with a compile-time decimal scale (`ScalingFactor = 10^DecimalPrecision`).
- Usual arithmetic (`+`, `-`, `*`, `/` against scalars, comparisons), conversions to/from `double`/`i64`, `round(...)`, and a specialization of `std::numeric_limits::max()`.

**`util.h`** — Low-level numeric primitives.
- `BitCast<TTo>(src)` — type-punning without UB (predates C++20 `std::bit_cast`).
- `Midpoint(a, b)` — overflow-safe midpoint (predates C++20 `std::midpoint`).
- `SignedSaturationArithmeticMultiply` / `Add` — saturating on overflow to `INT64_MIN`/`INT64_MAX`.
- `UnsignedSaturationArithmeticMultiply` / `Add(lhs, rhs, max = INT64_MAX)` — saturating to a configurable max.
- `SignedSaturationConversion(double)` — rounds toward zero with saturation to `i64` range.

**`serialize/`** — Opt-in serialization adapters for the headers above. Pull in as needed — the core headers intentionally don't depend on the YSON/native-serialize machinery.
- `serialize/double_array.h` — `FormatValue` for `TDoubleArray` (for `Format("%v", ...)`) and `operator<<` for `std::ostream`.
- `serialize/fixed_point_number.h` — YSON `Serialize`/`Deserialize` (as `double`), `FormatValue`, and a native-stream `TSerializerTraits` that persists the underlying integer.

**Notes:**
- `TPiecewiseLinearFunction` requires `TValue` to support linear interpolation (`operator+`, `operator*` with `double`). Common instantiations are `TPiecewiseLinearFunction<double>` and `TPiecewiseLinearFunction<TResourceVector>` (see `vector_hdrf`).
- `FloatingPointLowerBound` operates on the IEEE 754 bit representation, so it finds the exact boundary representable `double` — more precise than a conventional `(lo+hi)/2` loop.
- Most types are templates defined in `*-inl.h` files; include the primary header and the inline gets picked up automatically.
- `ClearAndSortCriticalPoints` is the only non-trivial runtime function in the library. Its merge fast path helps when callers concatenate multiple already-sorted sublists (the common case in `vector_hdrf`).

**See also:** `yt/yt/library/vector_hdrf` (primary consumer; builds resource vectors on top of `TDoubleArray` and fair-share curves on top of `TPiecewiseLinearFunction`).
