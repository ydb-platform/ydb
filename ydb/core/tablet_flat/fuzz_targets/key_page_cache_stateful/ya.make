FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/tablet_flat
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

END()
