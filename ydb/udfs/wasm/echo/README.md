# Echo service module

This second adapter exercises the same generic FQ transform with String input,
empty/binary strings, Uint64, Uint32, Bool and signed Int64 output. The manifest
registers methods `Echo` (ID 7) and `Length` (ID 9), with different result schemas.
See `query.sql`. Its HTTP endpoint echoes opaque bytes: a uint32 count followed
by length-prefixed binary strings. One RPC serves a scalar or batch invocation.
The adapter validates the complete echo/correlation before returning typed rows.
It supports HTTP only and fails unsupported protocols with a numeric error.

```bash
./ya make --build release --target-platform=clang20-emscripten-wasm64 ydb/udfs/wasm/echo
cp -L ydb/udfs/wasm/echo/libwasm-echo.so ydb/udfs/wasm/echo/ut/data/echo.wasm
chmod 644 ydb/udfs/wasm/echo/ut/data/echo.wasm
```

Register the built artifact and `manifest.json` in `WasmServices.Modules`, and
point an operator-owned binding at the HTTP echo endpoint. No Profile knowledge
or module-specific changes are needed in FQ or the async runtime.
