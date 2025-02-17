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
    yql/essentials/core/file_storage
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/utils
    yql/essentials/utils/log
    yql/essentials/core
    yql/essentials/providers/common/mkql
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/schema/expr
)

YQL_LAST_ABI_VERSION()

END()
