LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    GLOBAL safe_assert.cpp
)

PEERDIR(
    yt/yt/library/coredumper
)

END()
