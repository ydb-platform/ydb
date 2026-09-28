DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/udf_store/examples/sdk/webassembly_udf.inc)

STRIP()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
    library/cpp/digest/md5
)

END()
