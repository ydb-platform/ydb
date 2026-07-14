BUILD_ONLY_IF(OS_EMSCRIPTEN)

DLL()

# INCLUDE(${ARCADIA_ROOT}/yt/yt/experiments/private/ql_webassembly_udfs/sdk/webassembly_udf.inc)

LD_PLUGIN(yql/essentials/udfs/common/wasm/sdk/ld_plugin.py)

NO_UTIL()
NO_RUNTIME()
NO_LIBC()
STRIP()

# EXPORTS_SCRIPT(dll.exports)

IF(PROTOBUF_LITE)
    PEERDIR(contrib/libs/protobuf_std)
ELSE()
    PEERDIR(contrib/libs/protobuf)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    yql/essentials/udfs/common/wasm/abi
    yql/essentials/udfs/common/wasm/examples/proto_simple/gen/proto_schema
)

CFLAGS(
    -matomics
    -mbulk-memory
    -Oz
    -g0
    -flto=thin
    -fvisibility=hidden
)

LDFLAGS(
  -Wl,--allow-undefined
  -Wl,--export=proto_roundtrip
  -Wl,--export=malloc
  -Wl,--export=__heap_base
  -Wl,--export=__data_end
  -Wl,--export=__wasm_call_ctors
  -Wl,--export=_initialize
  -Wl,--initial-heap=16777216
  -flto=thin
  -fvisibility=hidden
  -Wl,-O3
)

END()

RECURSE(
    gen
)
