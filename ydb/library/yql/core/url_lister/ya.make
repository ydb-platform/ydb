LIBRARY()

SRCS(
    url_lister_manager.cpp
)

PEERDIR(
    ydb/library/yql/ast
    ydb/library/yql/core/url_lister/interface
)

END()

RECURSE(
    interface
)
