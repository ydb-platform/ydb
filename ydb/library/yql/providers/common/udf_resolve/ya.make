LIBRARY()

SRCS(
    yql_files_box.cpp
    yql_files_box.h
    yql_outproc_udf_resolver.cpp
    yql_outproc_udf_resolver.h
    yql_simple_udf_resolver.cpp
    yql_simple_udf_resolver.h
    yql_udf_resolver_with_index.cpp
    yql_udf_resolver_with_index.h
)

PEERDIR(
    library/cpp/digest/md5
    library/cpp/protobuf/util
    ydb/library/yql/core/file_storage
    ydb/library/yql/minikql
    ydb/library/yql/public/udf
    ydb/library/yql/utils
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/providers/common/mkql
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/schema/expr
)

YQL_LAST_ABI_VERSION()

END()
