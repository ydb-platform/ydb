PY3_PROGRAM(upload_udf)

PY_SRCS(
    __main__.py
)

PEERDIR(
    ydb/tests/functional/udf_store/lib
    ydb/tests/oss/ydb_sdk_import
)

DEPENDS(
    ydb/tests/stress/kv_volume_tool
)

END()
