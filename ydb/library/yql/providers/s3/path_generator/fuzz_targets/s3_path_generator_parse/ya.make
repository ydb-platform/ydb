FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/library/yql/providers/s3/path_generator
    yql/essentials/minikql
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
