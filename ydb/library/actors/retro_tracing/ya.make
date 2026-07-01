LIBRARY()

PEERDIR(
    ydb/library/actors/retro_tracing/collector
    ydb/library/actors/retro_tracing/span
)

END()

RECURSE(
    collector
    span
)

RECURSE_FOR_TESTS(
    ut
)
