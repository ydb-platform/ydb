G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)

SRCS(
    b_charge.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/scheme
    ydb/core/tablet_flat/test/libs/exec
    ydb/core/tablet_flat/test/libs/table
    ydb/core/testlib/default
)

END()
