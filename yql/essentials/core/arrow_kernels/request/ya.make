LIBRARY()

SRCS(
    request.cpp
)

PEERDIR(
    yql/essentials/ast
    yql/essentials/minikql
    yql/essentials/providers/common/mkql
    yql/essentials/core
    yql/essentials/sql
)

YQL_LAST_ABI_VERSION()

GENERATE_ENUM_SERIALIZATION(request.h)

END()
