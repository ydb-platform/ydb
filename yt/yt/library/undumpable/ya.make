LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    undumpable.cpp
    ref.cpp
)

PEERDIR(
    library/cpp/yt/assert
    library/cpp/yt/threading
    library/cpp/yt/memory
    library/cpp/yt/misc

    yt/yt/library/profiling
)

END()

RECURSE(unittests)

