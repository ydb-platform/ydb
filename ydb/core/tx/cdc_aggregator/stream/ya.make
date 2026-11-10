LIBRARY()

SRCS(
    activate_usecase.cpp
    configure_usecase.cpp
    replace_source_usecase.cpp
    report_delivered_usecase.cpp
    stream.cpp
    stream_config.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/scheme
)

END()

RECURSE_FOR_TESTS(
    ut
)
