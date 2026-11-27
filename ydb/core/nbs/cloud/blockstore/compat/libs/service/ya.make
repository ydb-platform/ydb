LIBRARY()

GENERATE_ENUM_SERIALIZATION(request.h)

SRCS(
    context.cpp
    request_helpers.cpp
    request.cpp
)

PEERDIR(
    ydb/core/nbs/cloud/blockstore/compat/config
    ydb/core/nbs/cloud/blockstore/compat/libs/common
    ydb/core/nbs/cloud/blockstore/compat/public/api/protos
    ydb/core/nbs/cloud/storage/core/libs/common
    ydb/core/nbs/cloud/storage/core/compat/libs/common

    library/cpp/lwtrace
)

END()

RECURSE_FOR_TESTS(ut)
