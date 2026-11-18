DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/udfs/wasm/common/webassembly_udf.inc)

CFLAGS(-fno-exceptions -fno-rtti)

SRCS(main.cpp)

PEERDIR(
    ydb/services/udf_store/wasm/abi
)

END()
