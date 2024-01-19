G_BENCHMARK()

TAG(ya:fat)
SIZE(LARGE)
TIMEOUT(1200)

SRCS(
    b_charge.cpp
    b_part_index.cpp
)

PEERDIR(
    library/cpp/resource
    ydb/core/scheme
    ydb/core/tablet_flat/test/libs/exec
    ydb/core/tablet_flat/test/libs/table
    ydb/core/testlib/default
)

END()
