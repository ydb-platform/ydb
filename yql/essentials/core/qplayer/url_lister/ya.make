LIBRARY()

ENABLE(YQL_STYLE_CPP)

SRCS(
    qplayer_url_lister_manager.cpp
)

PEERDIR(
    yql/essentials/core/qplayer/storage/interface
    yql/essentials/core/url_lister/interface
    yql/essentials/core
    library/cpp/yson/node
    contrib/libs/openssl
)

END()

