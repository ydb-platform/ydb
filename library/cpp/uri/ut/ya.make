UNITTEST_FOR(library/cpp/uri)

NO_OPTIMIZE()

NO_WSHADOW()

PEERDIR(
    library/cpp/html/entity
)

SRCS(
    location_ut.cpp
    uri-ru_ut.cpp
    uri_ut.cpp
)

END()
