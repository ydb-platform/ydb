LIBRARY()

# Type definitions and implementations have moved to ydb/core/kqp/opt/cbo/.
# This library is now a thin alias wrapper — no SRCS, just re-exports the ydb libraries.

PEERDIR(
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/optimizer
)

END()

RECURSE(
    simple
)

RECURSE_FOR_TESTS(
    ut
)
