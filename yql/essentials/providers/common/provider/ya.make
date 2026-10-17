YQL_LIBRARY()

SRCS(
    yql_data_provider_impl.cpp
    yql_data_provider_impl.h
    yql_provider.cpp
    yql_provider.h
    yql_provider_names.h
)

PEERDIR(
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/core
    yql/essentials/parser/pg_catalog
)

END()
