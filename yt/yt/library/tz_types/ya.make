LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    tz_types.cpp
)

PEERDIR(
    library/cpp/type_info/tz
    library/cpp/yt/memory
    library/cpp/yt/error
)

END()

RECURSE_FOR_TESTS(
    unittests
)
