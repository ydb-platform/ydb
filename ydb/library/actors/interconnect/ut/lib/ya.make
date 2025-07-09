LIBRARY()

SRCS(
    node.h
    test_events.h
    test_actors.h
    ic_test_cluster.h
)

PEERDIR(
    ydb/library/actors/interconnect/ut/lib/tls
)

END()
