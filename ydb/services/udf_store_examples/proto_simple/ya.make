BUILD_ONLY_IF(OS_EMSCRIPTEN)

DLL()

LD_PLUGIN(ydb/services/udf_store/wasm/sdk/ld_plugin.py)
LD_PLUGIN(ydb/services/udf_store/wasm/protobuf/ld_plugin.py)

NO_UTIL()
NO_RUNTIME()
NO_LIBC()
STRIP()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
    ydb/services/udf_store_examples/proto_simple/gen/proto_schema
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
