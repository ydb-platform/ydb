YQL_LIBRARY()

SRCS(
    yql_data_provider_impl.cpp
    yql_data_provider_impl.h
    yql_provider.cpp
    yql_provider.h
    yql_provider_names.h
    yql_provider_names.cpp
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/core
    yql/essentials/parser/pg_catalog
)

PEERDIR_FROM_POLICY(
    ALLOW
    logos/projects/ads/mldwh/querygen
    toloka/analytics/programs/yql_parser
)

END()
