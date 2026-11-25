DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/udfs/wasm/common/webassembly_udf.inc)

CFLAGS(-fno-exceptions -fno-rtti)

SRCS(
    main.cpp
    contrib/restricted/emscripten/system/lib/libc/musl/src/string/memcmp.c
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
    ydb/udfs/wasm/echo/contract
)

END()
