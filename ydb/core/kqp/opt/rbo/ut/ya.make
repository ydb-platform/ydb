UNITTEST()

SRCS(
    kqp_info_unit_ut.cpp
    kqp_read_ranges_ut.cpp
)

PEERDIR(
    library/cpp/containers/absl
    library/cpp/containers/stack_vector
    ydb/core/kqp/opt/rbo
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
