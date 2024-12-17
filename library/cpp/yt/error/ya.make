LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/global
    library/cpp/yt/memory
    library/cpp/yt/misc
    library/cpp/yt/threading
    library/cpp/yt/string
    library/cpp/yt/yson_string # TODO(arkady-e1ppa): eliminate
)

SRCS(
    error_attributes.cpp
    origin_attributes.cpp
)

END()
