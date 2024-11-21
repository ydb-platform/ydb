LIBRARY()

SRCS(
    logging_resolver.h
    logging_resolver_env_mock.cpp
)

PEERDIR(
    library/cpp/threading/future
    yql/essentials/public/issue
)

YQL_LAST_ABI_VERSION()

END()