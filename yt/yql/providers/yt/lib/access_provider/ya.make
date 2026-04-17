LIBRARY()

GENERATE_ENUM_SERIALIZATION(yt_access_provider.h)

PEERDIR(
    yql/essentials/core
    yql/essentials/providers/common/proto
    yt/yql/providers/yt/lib/tvm_client
)

END()

RECURSE(
    dummy
    full
)

IF (NOT OPENSOURCE)
    RECURSE(idm)
ENDIF()
