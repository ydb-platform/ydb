LIBRARY()

INCLUDE(${ARCADIA_ROOT}/yt/ya_cpp.make.inc)

SRCS(
    config.cpp
    coredumper.cpp
)

PEERDIR(
    yt/yt/core
    yt/yt/library/sparse_coredump
)

IF (OS_LINUX AND NOT OPENSOURCE AND NOT SANITIZER_TYPE)
    SRCS(
        GLOBAL coredumper_impl.cpp
    )
    PEERDIR(
        yt/yt/contrib/coredumper
    )
ENDIF()

END()
