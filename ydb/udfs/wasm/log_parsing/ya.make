DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/udfs/wasm/common/webassembly_udf.inc)

STRIP()

SRCS(
    line_break.cpp
    main.cpp
    parse_tskv.cpp
    protoseq.cpp
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
)

END()
