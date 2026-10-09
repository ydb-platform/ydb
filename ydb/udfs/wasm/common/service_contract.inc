RUN_PYTHON3(
    ${ARCADIA_ROOT}/ydb/udfs/wasm/sdk/services/generate.py
        ${ARCADIA_ROOT}/${MODDIR}/../manifest.json
        ${BINDIR}/service_methods.h
    IN ${ARCADIA_ROOT}/${MODDIR}/../manifest.json
    OUT service_methods.h
)
