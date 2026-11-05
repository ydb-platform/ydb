UNITTEST()

SRCS(
    session_id_ut.cpp
)

PEERDIR(
    library/cpp/string_utils/base64
    library/cpp/string_utils/quote
    ydb/core/kqp/common/simple
)

END()
