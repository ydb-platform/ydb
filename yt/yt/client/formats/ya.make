LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    format.cpp
    parser.cpp
    schemaful_writer.cpp
    versioned_writer.cpp
)

PEERDIR(
    yt/yt/client
    yt/yt/library/skiff_ext
    yt/yt_proto/yt/formats
    library/cpp/string_utils/base64
)

END()
