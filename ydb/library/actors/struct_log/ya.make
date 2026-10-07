LIBRARY()

SRCS(
    create_message.cpp
    json_writer.cpp
    key_name.cpp
    log_stack.cpp
    meta_writer.cpp
    native_types_support.cpp
    structured_message.cpp
    text_writer.cpp
)

PEERDIR(
    ydb/library/services
)

END()
