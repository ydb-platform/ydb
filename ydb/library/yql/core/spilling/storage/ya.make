LIBRARY()

SRCS(
    storage.h
    storage.cpp
)

PEERDIR(
    ydb/library/yql/core/spilling/storage/file_storage
)

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
