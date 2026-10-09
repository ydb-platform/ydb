RUN_PYTHON3(
    ${ARCADIA_ROOT}/ydb/udfs/wasm/sdk/services/generate.py
        ${ARCADIA_ROOT}/${MODDIR}/../service.json
        ${BINDIR}/manifest.json
        ${BINDIR}/service_methods.h
    IN ${ARCADIA_ROOT}/${MODDIR}/../service.json
    OUT service_methods.h
    OUT_NOAUTO manifest.json
)

IF (NOT OS_EMSCRIPTEN)
    RESOURCE(${BINDIR}/manifest.json /${MODDIR}/manifest.json)
    PEERDIR(library/cpp/resource)
ENDIF()
