PY3_LIBRARY()
PY_SRCS(mock.py config.py)
PEERDIR(
    contrib/python/grpcio
    ydb/udfs/wasm/profile/proto/schema
)
END()
