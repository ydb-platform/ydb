RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
	source.cpp
	support_links_resolver.cpp
)

PEERDIR(
	ydb/mvp/core
	ydb/mvp/meta/protos
)

YQL_LAST_ABI_VERSION()

END()
