LIBRARY()

SRCS(
    dnscache.cpp
    dnscache.h
    probes.cpp
    probes.h
    timekeeper.h
)

PEERDIR(
    contrib/libs/c-ares
    library/cpp/lwtrace
    library/cpp/deprecated/atomic
)

IF (NOT EXPORT_CMAKE)
    ADDINCL(
        contrib/libs/c-ares/include
    )
ENDIF()

END()
