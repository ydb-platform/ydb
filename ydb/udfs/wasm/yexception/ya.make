DLL()

INCLUDE(${ARCADIA_ROOT}/ydb/udfs/wasm/common/webassembly_udf.inc)

# Keep DWARF and avoid inlining so trap stacks show boom_leaf → boom_middle → fail.
CFLAGS(
    -g
    -O0
    -fno-inline
)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/services/udf_store/wasm/abi
)

END()
