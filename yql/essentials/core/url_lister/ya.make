LIBRARY()

SRCS(
    url_lister_manager.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/core/url_lister/interface
    yql/essentials/utils/fetch
)

END()

RECURSE(
    interface
)

