PY23_LIBRARY()

PY_SRCS(
    control_plane.py
    data_plane.py
    test_yds_base.py
)

PEERDIR(
    ydb/public/api/grpc/draft
    ydb/public/api/protos
)

END()
