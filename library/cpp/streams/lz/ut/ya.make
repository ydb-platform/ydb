UNITTEST_FOR(library/cpp/streams/lz)

RESOURCE(
    random.data /random.data
    request.data /request.data
    yq_609.data /yq_609.data
)

IF(OPENSOURCE)
    CFLAGS(-DOPENSOURCE)
ENDIF()

SRCS(
    lz_ut.cpp
)

END()
