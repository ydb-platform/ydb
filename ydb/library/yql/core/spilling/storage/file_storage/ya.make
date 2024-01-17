LIBRARY()

SRCS(
    file_storage.cpp
)

PEERDIR(
    ydb/library/yql/utils/log
)

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()

