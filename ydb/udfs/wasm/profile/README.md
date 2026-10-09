# Profile service module

This module owns the private Profile domain model, HTTP JSON and gRPC protobuf
request/response schemas, strict decoding, UTF-8/score/version checks, batch
correlation and duplicate-ID preservation. Its manifest keeps the existing
`EXTERNAL FUNCTION('WASM_PROFILE', 'Profile')` SQL name. See `query.sql`.

Scalar HTTP sends `{"id":42}` and expects a Profile object. Batch HTTP sends
`{"ids":[42,43]}` and expects an ordered array of equally many Profile objects.
Use `/profile` and `/profile/batch` on the test mock respectively. gRPC uses
`Lookup`/`LookupBatch` and the messages in `proto/schema/profile.proto`.
Each batch uses one RPC; a bad/missing/extra/reordered item fails the whole call.

Legacy fixture modes still exercise P1 single/sequential/parallel native calls
and typed Profile decoding in infrastructure tests. Analytical queries use
the generic row ABI from `sdk/services/rows.h`, not those legacy fixture modes.

From the repository root:

```bash
./ya make --build relwithdebinfo contrib/tools/protoc contrib/libs/nanopb/generator
contrib/tools/protoc/protoc -I ydb/udfs/wasm/profile/proto/schema \
    --plugin=protoc-gen-nanopb="$PWD/contrib/libs/nanopb/generator/generator" \
    --nanopb_opt=-fydb/udfs/wasm/profile/proto/profile.options \
    --nanopb_out=ydb/udfs/wasm/profile/proto profile.proto
./ya make --build release --target-platform=clang20-emscripten-wasm64 ydb/udfs/wasm/profile
cp -L ydb/udfs/wasm/profile/libwasm-profile.so ydb/udfs/wasm/profile/ut/data/profile.wasm
chmod 644 ydb/udfs/wasm/profile/ut/data/profile.wasm
```

Generated nanopb files and the regenerated WASM test artifact are checked in.
Native protobuf and Python messages are built separately from `proto/schema`.
