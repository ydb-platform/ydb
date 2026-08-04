FUZZ()

SRCS(
    main.cpp
)

PEERDIR(
    ydb/core/kafka_proxy
    yql/essentials/minikql
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
