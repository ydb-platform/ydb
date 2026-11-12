LIBRARY()

SET(WASM_SERVICE_MANIFEST ${ARCADIA_ROOT}/ydb/udfs/wasm/echo/manifest.json)

INCLUDE(${ARCADIA_ROOT}/ydb/udfs/wasm/common/service_contract.inc)

SRCS(contract.cpp)

END()
