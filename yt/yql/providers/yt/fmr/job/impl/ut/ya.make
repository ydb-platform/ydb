UNITTEST()

SRCS(
    yql_yt_job_ut.cpp
    yql_yt_map_job_ut.cpp
    yql_yt_raw_table_queue_ut.cpp
    yql_yt_table_data_service_reader_ut.cpp
    yql_yt_table_data_service_writer_ut.cpp
)

PEERDIR(
    yt/cpp/mapreduce/common
    yt/yql/providers/yt/fmr/job/impl
    yt/yql/providers/yt/fmr/process
    yt/yql/providers/yt/fmr/test_tools/table_data_service
    yt/yql/providers/yt/fmr/test_tools/yson
    yt/yql/providers/yt/fmr/yt_job_service/mock
    yt/yql/providers/yt/fmr/utils
    yql/essentials/utils/log
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
    yql/essentials/minikql/comp_nodes/llvm16
    yt/yql/providers/yt/comp_nodes/llvm16
)

YQL_LAST_ABI_VERSION()

END()
