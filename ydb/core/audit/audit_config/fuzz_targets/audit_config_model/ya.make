FUZZ()

SIZE(SMALL)

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/audit/audit_config
    ydb/core/protos
    ydb/library/aclib/protos/identity
)

END()
