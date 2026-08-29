LIBRARY()

PEERDIR(
    ydb/core/persqueue/events
    ydb/core/persqueue/public/dataplane/write
)

END()

RECURSE(
    write
)
