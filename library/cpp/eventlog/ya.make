LIBRARY()

PEERDIR(
    library/cpp/blockcodecs
    library/cpp/eventlog/proto
    library/cpp/json
    library/cpp/logger
    library/cpp/protobuf/json
    library/cpp/streams/growing_file_input
    library/cpp/string_utils/base64
    contrib/libs/re2
)

SRCS(
    common.h
    evdecoder.cpp
    event_field_output.cpp
    event_field_printer.cpp
    eventlog.cpp
    eventlog_int.cpp
    iterator.cpp
    logparser.cpp
    threaded_eventlog.cpp
)

GENERATE_ENUM_SERIALIZATION(eventlog.h)
GENERATE_ENUM_SERIALIZATION(eventlog_int.h)

END()
