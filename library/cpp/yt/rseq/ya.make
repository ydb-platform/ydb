LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

SRCS(
    rseq.cpp
)

PEERDIR(
    library/cpp/yt/misc
)

END()
