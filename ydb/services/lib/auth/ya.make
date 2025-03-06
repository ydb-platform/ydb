LIBRARY()

SRCS(
    auth_helpers.cpp
)

PEERDIR(
    ydb/library/aclib
    ydb/core/base
    ydb/core/tx/scheme_cache
)

END()
