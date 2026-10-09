DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/udfs/wasm/common/webassembly_udf.inc)

CFLAGS(-fno-exceptions -fno-rtti)

ADDINCL(contrib/libs/rapidjson/include)

SRCS(main.cpp)

PEERDIR(
    contrib/libs/rapidjson
    ydb/udfs/wasm/profile/proto
    ydb/services/udf_store/wasm/abi
)

END()
