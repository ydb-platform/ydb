LIBRARY()

SRCS(
    ut_common.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/testing/common

    ydb/core/base

    ydb/core/fq/libs/row_dispatcher/events
    ydb/core/fq/libs/row_dispatcher/format_handler/common

    ydb/library/actors/testlib
    ydb/library/yql/dq/actors
    ydb/library/yql/dq/common

    yql/essentials/minikql
    yql/essentials/minikql/computation
    yql/essentials/minikql/invoke_builtins
    yql/essentials/providers/common/schema/mkql
)

YQL_LAST_ABI_VERSION()

END()
