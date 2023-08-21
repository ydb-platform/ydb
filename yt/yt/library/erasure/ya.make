LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

PEERDIR(
    yt/yt/core
)

SRCS(
    public.cpp
)

END()

RECURSE(
    impl
)
