LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    url_lister.cpp
    url_lister_manager.cpp
)

PEERDIR(
    library/cpp/yson/node
    yql/essentials/core/credentials
    yql/essentials/core/url_preprocessing/interface
)

END()
