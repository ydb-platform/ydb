DLL()

INCLUDE(${ARCADIA_ROOT}/yql/essentials/udfs/common/wasm/sdk/webassembly_udf.inc)

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/json
    yql/essentials/udfs/common/wasm/abi
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
  -Wl,--export=malloc
  -Wl,--export=__heap_base
  -Wl,--export=__data_end
  -Wl,--export=__wasm_call_ctors
  -Wl,--export=_initialize
  -Wl,--initial-heap=16777216
  -flto=thin
  -Wl,-O2
)

END()
