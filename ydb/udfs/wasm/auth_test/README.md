# Synthetic WASM authorization fixture

This module implements the public auth ABI for tests. It is not a TVM client.
Operator approval of `TestAuth` is required; ordinary UDF upload does not grant
permission to register `TEST_AUTH`.

The host creates a private memory/linking realm in the application's WAVM
compartment. The module reads one secret through a scoped host handle, then
POSTs it to `/auth` on bootstrap binding 0 with alias `auth.test`. The response
is the `NAuth::TResult` envelope followed by a fake ticket. The host applies the
credential to `x-auth-test`; no secret or ticket is returned to application WASM.

Build the fixture using the repository WASM toolchain:

```bash
./ya make --build relwithdebinfo --target-platform=clang20-emscripten-wasm64 ydb/udfs/wasm/auth_test
cp -L ydb/udfs/wasm/auth_test/libwasm-auth_test.so ydb/udfs/wasm/auth_test/ut/data/auth_test.wasm
```

The ABI uses the existing async start/poll/cancel/drop exports. The request
contains public JSON parameters followed by opaque secret handles in manifest
order. The result declares a bounded printable credential and its lifetime,
measured conservatively from refresh start. The `Secret` operation is rejected
by ordinary application runtimes even if they import its host function.
