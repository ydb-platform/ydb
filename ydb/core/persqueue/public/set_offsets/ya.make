LIBRARY()

SRCS(
    set_offsets.cpp
)

PEERDIR(
    library/cpp/containers/absl
    ydb/core/base
    ydb/core/persqueue/common
    ydb/core/persqueue/events
    ydb/core/persqueue/public
    ydb/core/persqueue/public/describer
    ydb/core/util
    ydb/library/actors/core
    ydb/library/aclib
    ydb/library/persqueue/topic_parser
    ydb/public/api/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
