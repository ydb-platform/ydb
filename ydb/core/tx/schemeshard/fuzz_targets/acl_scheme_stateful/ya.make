FUZZ()

SRCS(
    main.cpp
    ../../schemeshard_effective_acl.cpp
)

PEERDIR(
    ydb/library/aclib
)

YQL_LAST_ABI_VERSION()

END()
