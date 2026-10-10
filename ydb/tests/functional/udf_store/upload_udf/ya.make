PY3_PROGRAM(upload_udf)

STYLE_PYTHON()

PY_SRCS(
    __main__.py
)

REQUIREMENTS(cpu:1)
PEERDIR(
    ydb/tests/functional/udf_store/lib
    ydb/tests/oss/ydb_sdk_import
)

NO_CHECK_IMPORTS(
    *ydb.tests.oss.canonical.*
)

DEPENDS(
    ydb/tests/stress/kv_volume_tool
)

END()
