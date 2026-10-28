# FP8 vector support implementation plan

## Implementation status — 2026-10-28

Steps 1–6 are implemented for unscaled E4M3FN and E5M2, including portable conversion, SSE/AVX2 distance kernels, Knn UDFs, vector indexes, K-means, public protobufs, the C++ SDK, and workload generation. The conversion policy and identifiers described below were adopted.

Completed validation:

| Check | Result |
|---|---|
| Numeric conversion and scalar/SIMD distances | 4 tests passed, including exhaustive encoding and rounding checks |
| AddressSanitizer numeric suite | 4 tests passed |
| Knn SQL suite | All 24 cases passed against canonical results |
| K-means suite | 48 tests passed |
| C++ SDK table suite | 20 tests passed with warnings treated as errors |
| Vector workload generation | 4 tests passed |
| SHOW CREATE TABLE formatter | 12 tests passed |
| KQP search pushdown | 5 tests passed, including both FP8 formats |
| Vector indexes | 6 tests passed, covering both FP8 formats with and without prefixes, plus existing half formats |

The SDK dependency check passed. Full SDK clang-tidy still reports existing findings in unchanged code; it reported no diagnostics on the new FP8 mappings. Step 7 documentation and end-to-end recall/performance benchmarks remain outside this implementation.

## Scope and references

Support both E4M3 and E5M2 for exact vector search and vector indexes, following the scope of these pull requests:

- [#53023: Add Float16 and BFloat16 vector formats to Knn UDF](https://github.com/ydb-platform/ydb/pull/53023): numeric representations, distance kernels, serialization, and Knn UDFs.
- [#53242: Added Float16 and BFloat16 vector index types](https://github.com/ydb-platform/ydb/pull/53242): vector indexes, K-means, search pushdown, API, SDK, and workloads.
- [#53289: Docs: Added Float16 and BFloat16 vector index types](https://github.com/ydb-platform/ydb/pull/53289): English and Russian documentation.

The names, identifiers, and conversion policy below are the implemented contract. The identifiers were checked against the checkout before assignment.

## 1. Define the FP8 contract

Use E4M3FN for the requested E4M3 format, and standard E5M2. Document this explicitly: FNUZ variants have different encodings and exponent biases. See the [ONNX FP8 specification](https://onnx.ai/onnx/technical/float8.html).

| Property | E4M3, implemented as E4M3FN | E5M2 |
|---|---:|---:|
| Sign / exponent / fraction bits | 1 / 4 / 3 | 1 / 5 / 2 |
| Exponent bias | 7 | 15 |
| Maximum finite magnitude | 448 | 57344 |
| Smallest positive subnormal | 2⁻⁹ | 2⁻¹⁶ |
| Encodes infinity | No | Yes |
| Encodes NaN and signed zero | Yes | Yes |

Conversion policy: round-to-nearest, ties-to-even, with saturation. Overflow and input infinities become the signed maximum finite value; NaNs remain NaNs. Decoding must recognize every encoding, including E5M2 infinities. Specify this policy consistently in code, tests, and documentation.

Start with unscaled FP8, matching the existing FP16 storage model. Per-vector or block scaling requires a separate storage and distance contract.

## 2. Add portable numeric types and distance kernels

In [ydb/library/vector_distance](ydb/library/vector_distance), add `float8.h` containing `TFloat8E4M3` and `TFloat8E5M2`.

Each type should have one-byte storage, explicit conversion to/from `float`, and a way to construct it from raw bits. Assert its size and trivial copyability. Implement conversion directly from FP32 to avoid intermediate FP16 rounding. Use C++20 or earlier.

Add `float8_distance.h/.cpp` with overloads for L1, squared L2, L2, dot product, and the three dot products used by cosine similarity. Decode coordinates while computing and accumulate in FP32, without allocating expanded vectors.

Establish a portable scalar implementation first. Then add SSE/AVX2 implementations with runtime dispatch, following the existing half-distance structure. Benchmark bit-based decoding against lookup-table decoding before selecting the optimized implementation; preserve the scalar fallback.

## 3. Expose both formats through Knn UDFs and serialization

Extend [knn-defines.h](ydb/library/yql/udfs/common/knn/knn-defines.h), the serializer facade, distance dispatch, and UDF registration.

Public interface:

```yql
Knn::ToBinaryStringFloat8E4M3(List<Float>)
    -> Tagged<String, "Float8E4M3Vector">

Knn::ToBinaryStringFloat8E5M2(List<Float>)
    -> Tagged<String, "Float8E5M2Vector">
```

Preserve the existing binary layout: **N coordinate bytes followed by one format byte**. Format identifiers are `6` for E4M3 and `7` for E5M2.

Reuse the generic serializer through `TTypeToFormat` specializations. Extend `FloatFromBinaryString`, all distance functions, accepted-tag lists, and diagnostic messages.

Keep existing comparison rules: distinct tagged formats produce a type error; distinct formats supplied as untagged strings return `NULL`. FP8 does not require new YQL scalar types. Preserve existing AutoMap, strictness, null, and optional-argument behavior.

## 4. Integrate vector indexes and K-means

In [kmeans_clusters.cpp](ydb/core/base/kmeans_clusters.cpp), extend `ParseVectorType`, `CreateClusters`, `CreateClustersAutoDetect`, and `AutoSelectVectorSettings` for:

```text
vector_type = float8_e4m3
vector_type = float8_e5m2
```

Dimension detection becomes `payload size = string size − 1`. Validate the format independently of the byte width: FP8, Int8, and Uint8 all occupy one byte per coordinate.

Generalize the current hardcoded FP16/BFloat16 arithmetic selection into a small trait shared by reduced-precision floating types. Both FP8 types must use FP32 arithmetic and FP64 centroid sums.

Store centroids in the selected FP8 format, as FP16 indexes currently do for their formats. Accumulate weighted coordinates and perform normalization in higher precision, then quantize when writing the centroid. Test this explicitly to prevent FP8 accumulation, integer truncation, or repeated quantization.

Search pushdown and runtime cluster ranking already use `CreateClustersAutoDetect`; verify these paths end to end.

## 5. Complete API, SDK, and tooling support

Extend [ydb_table.proto](ydb/public/api/protos/ydb_table.proto) with these values:

```protobuf
VECTOR_TYPE_FLOAT8_E4M3 = 7;
VECTOR_TYPE_FLOAT8_E5M2 = 8;
```

These are separate from the binary format identifiers. Preserve all existing enum values.

Update both C++ SDK vector-type enums, protobuf conversions in both directions, stream formatting, and the changelog. Extend `SHOW CREATE TABLE` formatting and workload option parsing, bulk generation, and query generation. Verify create/describe and DDL round trips preserve the selected format.

Relevant locations:

- [C++ SDK table interface](ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h)
- [C++ SDK table implementation](ydb/public/sdk/cpp/src/client/table/table.cpp)
- [C++ SDK output formatting](ydb/public/sdk/cpp/src/client/table/out.cpp)
- [SHOW CREATE TABLE formatter](ydb/core/sys_view/show_create/formatters/create_table_formatter.cpp)
- [Vector workloads](ydb/library/workload/vector)

## 6. Add correctness and integration tests

Add numeric and distance unit tests under `ydb/library/vector_distance/ut`, SQL cases and canonical results under `ydb/library/yql/udfs/common/knn/test`, and extend the existing K-means, index, and pushdown suites.

Coverage should include:

- All 256 encodings per format, finite-value round trips, NaNs, infinities, and signed zero.
- Rounding midpoints and adjacent FP32 values, subnormal boundaries, underflow, and saturation.
- Every metric against an FP32 reference constructed by **decoding the stored FP8 inputs**.
- SIMD boundaries, tails, unaligned input, scalar/optimized agreement, and portable builds.
- Null/optional arguments, empty input, unknown formats, length mismatches, and cross-format comparisons.
- Weighted centroids whose sums exceed FP8 range, fractional means, cosine normalization, and zero vectors.
- Explicit and inferred index settings, index creation on populated tables, subsequent updates, prefixed indexes, indexed search, and pushdown.
- SDK protobuf conversions in both directions, type formatting, and create/describe/SHOW CREATE round trips.

Existing integration test locations:

- [K-means tests](ydb/core/base/ut/kmeans_ut.cpp)
- [Vector index tests](ydb/core/kqp/ut/indexes/vector/kqp_indexes_vector_ut.cpp)
- [Knn pushdown tests](ydb/core/kqp/ut/knn/kqp_knn_ut.cpp)

Run the affected targets using the repository build conventions, with focused filters for the larger suites:

```bash
./ya make --build relwithdebinfo -tA ydb/library/vector_distance/ut 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/library/yql/udfs/common/knn/test 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/core/base/ut -F 'NKMeans::*' 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/indexes/vector -F '*Float8*' 2>&1 | tail
./ya make --build relwithdebinfo -tA ydb/core/kqp/ut/knn -F '*Float8*' 2>&1 | tail
```

Use final test names when choosing filters, and verify that the intended tests actually ran. Tests include the build; do not use `-j`, force rebuilds, or edit sources during compilation. Complete appropriate SDK, formatter, workload, and prefixed-index checks as those components change.

## 7. Document and measure the result

Update the same English and Russian pages as #53289: UDF signatures, binary identifiers, index parameters, and approximate-search formats. Explain E4M3FN semantics, rounding, saturation, quantization loss, and the absence of implicit scaling.

Relevant documentation paths, under both `ydb/docs/en/core` and `ydb/docs/ru/core`:

- `yql/reference/udf/list/knn.md`
- `yql/reference/syntax/_includes/vector_index_parameters.md`
- `yql/reference/_includes/vector_search_approximate.md`

Benchmark FP8 against Float, Float16/BFloat16, and Int8 for distance throughput, index build time, query latency, and recall. Measure recall against original FP32 vectors separately from numerical correctness against decoded FP8 vectors. Pay particular attention to small coordinates and cosine-centroid quantization.

## Delivery sequence

1. Numeric types, distance kernels, and UDFs, with conversion, kernel, and SQL tests.
2. Complete index, API, SDK, and workload support, with integration tests.
3. English and Russian documentation plus benchmark results.

Begin FP8 usage only after all participating server nodes support the new formats.
