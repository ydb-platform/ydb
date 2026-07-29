LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    helpers.cpp
    parser.cpp
    writer.cpp
)

PEERDIR(
    yt/yt/core

    contrib/libs/yaml
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    unittests
)
