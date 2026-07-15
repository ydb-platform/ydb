LIBRARY()

PEERDIR(
    ydb/library/aclib/protos/identity
    ydb/library/aclib/protos/acl
)

END()

RECURSE(
    identity
    acl
)
