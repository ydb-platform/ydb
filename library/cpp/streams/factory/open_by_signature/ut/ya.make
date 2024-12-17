UNITTEST_FOR(library/cpp/streams/factory/open_by_signature)

IF(OPENSOURCE)
    CFLAGS(-DOPENSOURCE)
ENDIF()

SRCS(
    factory_ut.cpp
)

END()
