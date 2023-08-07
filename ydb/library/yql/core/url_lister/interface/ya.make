LIBRARY()

SRCS(
    url_lister.cpp
    url_lister_manager.cpp
)

PEERDIR(
    library/cpp/uri
    library/cpp/yson/node
    ydb/library/yql/core/credentials
    ydb/library/yql/core/url_preprocessing/interface
)

END()
