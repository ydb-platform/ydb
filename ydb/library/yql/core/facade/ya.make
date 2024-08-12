LIBRARY()

SRCS(
    yql_facade.cpp
)

PEERDIR(
    library/cpp/deprecated/split
    library/cpp/random_provider
    library/cpp/string_utils/base64
    library/cpp/threading/future
    library/cpp/time_provider
    library/cpp/yson
    library/cpp/yson/node
    ydb/library/yql/core/extract_predicate
    ydb/library/yql/core/file_storage
    ydb/library/yql/core/services
    ydb/library/yql/core/url_lister/interface
    ydb/library/yql/core/url_preprocessing/interface
    ydb/library/yql/core/credentials
    ydb/library/yql/core/qplayer/storage/interface
    ydb/library/yql/core/qplayer/udf_resolver
    ydb/library/yql/sql
    ydb/library/yql/utils/log
    ydb/library/yql/core
    ydb/library/yql/core/type_ann
    ydb/library/yql/providers/common/config
    ydb/library/yql/providers/common/proto
    ydb/library/yql/providers/common/provider
    ydb/library/yql/providers/common/udf_resolve
    ydb/library/yql/providers/common/arrow_resolve
    ydb/library/yql/providers/config
    ydb/library/yql/providers/result/provider
)

YQL_LAST_ABI_VERSION()

END()
