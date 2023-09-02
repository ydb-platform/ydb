LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    io_dispatcher.cpp
    pipe.cpp
    process.cpp
    pty.cpp
    subprocess.cpp
)

PEERDIR(
    yt/yt/core
    contrib/libs/re2
)

END()

RECURSE_FOR_TESTS(
    unittests
)
