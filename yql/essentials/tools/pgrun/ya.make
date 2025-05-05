IF (NOT OPENSOURCE)

PROGRAM(pgrun)

ALLOCATOR(J)

SRCS(
    pgrun.cpp
)

PEERDIR(
    contrib/libs/protobuf
    library/cpp/getopt
    library/cpp/string_utils/base64
    contrib/libs/fmt
    library/cpp/yson
    library/cpp/yson/node
    yql/essentials/sql/pg
    yql/essentials/core/cbo/simple
    yql/essentials/core/facade
    yql/essentials/core/file_storage
    yql/essentials/core/file_storage/proto
    yql/essentials/core/file_storage/http_download
    yql/essentials/core/services/mounts
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/protos
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/utils/backtrace
    yql/essentials/core
    yql/essentials/sql/v1/format
    yql/essentials/providers/common/codec
    yql/essentials/providers/common/comp_nodes
    yql/essentials/providers/common/proto
    yql/essentials/providers/common/provider
    yql/essentials/providers/common/udf_resolve
    yt/yql/providers/yt/common
    yt/yql/providers/yt/lib/schema
    yt/yql/providers/yt/gateway/file
    yt/yql/providers/yt/comp_nodes/llvm16
    yt/yql/providers/yt/codec/codegen
    yql/essentials/providers/pg/provider
    yql/essentials/core/url_preprocessing
    yql/essentials/parser/pg_wrapper
)

YQL_LAST_ABI_VERSION()

END()

ENDIF()

