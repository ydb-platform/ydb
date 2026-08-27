BUILD_ONLY_IF(OS_EMSCRIPTEN)

DLL()

LD_PLUGIN(ydb/tests/functional/udf_store/examples/sdk/ld_plugin.py)

NO_UTIL()
NO_RUNTIME()
NO_LIBC()
STRIP()

SRCS(
    main.cpp
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
    -Wl,--export=helpers_scale
    -Wl,--export=malloc
    -Wl,--export=__heap_base
    -Wl,--export=__data_end
    -Wl,--export=__wasm_call_ctors
    -Wl,--export=_initialize
    -Wl,--initial-heap=16777216
    -flto=thin
    -Wl,-O2
    -fvisibility=hidden
)

END()
