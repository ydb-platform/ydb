BUILD_ONLY_IF(OS_EMSCRIPTEN)

DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/functional/udf_store/examples/sdk/webassembly_udf.inc)

STRIP()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
)

END()
