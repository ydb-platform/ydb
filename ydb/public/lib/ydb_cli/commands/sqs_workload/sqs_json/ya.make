LIBRARY(sqs_json)

SRCS(
    sqs_json_client.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-sqs
    library/cpp/string_utils/url
)

END()

