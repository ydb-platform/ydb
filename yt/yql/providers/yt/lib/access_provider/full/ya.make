LIBRARY()

SRCS(
    yt_access_provider_dummy.cpp
)

PEERDIR(
    yt/yql/providers/yt/lib/access_provider/dummy
    yt/yql/providers/yt/lib/access_provider/proto
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
