LIBRARY()

GENERATE_ENUM_SERIALIZATION(yt_access_provider.h)

PEERDIR(
    yql/essentials/core
    yt/yql/providers/yt/lib/tvm_client
)

END()

RECURSE(
    dummy
    full
    proto
)

IF (NOT OPENSOURCE)
    RECURSE(idm)
ENDIF()
