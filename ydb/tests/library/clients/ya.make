PY23_LIBRARY()

PY_SRCS(
    __init__.py
    kikimr_http_client.py
    kikimr_keyvalue_client.py
    kikimr_monitoring.py
    kikimr_scheme_client.py
)

PEERDIR(
    ydb/public/api/protos
    ydb/public/api/grpc
)

END()
