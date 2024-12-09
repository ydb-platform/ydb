LIBRARY()

INCLUDE(${ARCADIA_ROOT}/library/cpp/yt/ya_cpp.make.inc)

IF (OS_LINUX AND NOT SANITIZER_TYPE)
    SRCS(stockpile_linux.cpp)
ELSE()
    SRCS(stockpile_other.cpp)
ENDIF()

PEERDIR(
    library/cpp/yt/misc
    library/cpp/yt/threading
    library/cpp/yt/logging
    library/cpp/yt/memory
)

END()
