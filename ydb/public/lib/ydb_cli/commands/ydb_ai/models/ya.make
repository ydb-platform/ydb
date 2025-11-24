LIBRARY()

SRCS(
    model_anthropic.cpp
    model_base.cpp
    model_openai.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/writer
    library/cpp/string_utils/url
    library/cpp/threading/future
    ydb/library/yql/providers/common/http_gateway
    ydb/public/lib/ydb_cli/commands/ydb_ai/common
    ydb/public/lib/ydb_cli/common
)

END()
