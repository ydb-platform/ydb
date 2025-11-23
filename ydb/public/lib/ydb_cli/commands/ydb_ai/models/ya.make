LIBRARY()

SRCS(
    model_anthropic.cpp
    model_openai.cpp
)

PEERDIR(
    library/cpp/json
    library/cpp/json/writer
    library/cpp/string_utils/url
    library/cpp/threading/future
    ydb/library/yql/providers/common/http_gateway
)

END()
