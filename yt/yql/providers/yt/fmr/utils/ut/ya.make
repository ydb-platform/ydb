UNITTEST()

SRCS(
    yql_yt_parse_records_ut.cpp
)

PEERDIR(
    yt/yql/providers/yt/fmr/utils
    yt/yql/providers/yt/fmr/request_options
    yt/yql/providers/yt/fmr/yt_service/mock
    yql/essentials/parser/pg_wrapper
    yql/essentials/parser/pg_wrapper/interface
    yql/essentials/public/udf
    yql/essentials/public/udf/arrow
    yql/essentials/minikql/dom
    yql/essentials/public/udf/service/exception_policy
    yt/yql/providers/yt/job
    yql/essentials/sql/pg
    yt/yql/providers/yt/codec/codegen/llvm16
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/computation/llvm16
)

YQL_LAST_ABI_VERSION()

END()
