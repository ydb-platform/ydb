UNITTEST()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

FORK_SUBTESTS()

PEERDIR(
    ydb/public/sdk/cpp/src/client/extension_common
    ydb/public/sdk/cpp/src/client/table
)

SRCS(
    discovery_mutator_ut.cpp
)

END()
