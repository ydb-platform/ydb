LIBRARY()

SRCDIR(ydb/core/tx/columnshard/engines/scheme/indexes/abstract)

SRCS(
    fetcher.cpp
)

PEERDIR(
    ydb/core/tx/columnshard/engines/scheme/indexes/abstract
    ydb/core/tx/columnshard/engines/portions
    ydb/core/tx/columnshard/blobs_action/abstract
)

YQL_LAST_ABI_VERSION()

END()
