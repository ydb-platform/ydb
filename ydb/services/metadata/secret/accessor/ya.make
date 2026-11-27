LIBRARY()

SRCS(
    secret_id.cpp
)

PEERDIR(
    library/cpp/digest/md5
    ydb/core/base
    ydb/core/protos
    ydb/library/accessor
    ydb/library/aclib
)

END()
