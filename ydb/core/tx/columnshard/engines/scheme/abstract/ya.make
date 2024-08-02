LIBRARY()

SRCS(
    index_info.cpp
)

PEERDIR(
    ydb/library/actors/core
    ydb/core/formats/arrow/save_load
)

YQL_LAST_ABI_VERSION()

END()
