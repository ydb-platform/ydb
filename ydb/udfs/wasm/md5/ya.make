DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/udfs/wasm/common/webassembly_udf.inc)

STRIP()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
    library/cpp/digest/md5
)

END()
