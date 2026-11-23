RUN_PYTHON3(
    ${ARCADIA_ROOT}/ydb/udfs/wasm/sdk/services/generate.py
        ${WASM_SERVICE_MANIFEST}
        ${BINDIR}/service_methods.h
    IN ${WASM_SERVICE_MANIFEST}
    OUT service_methods.h
)
