UNITTEST_FOR(library/cpp/monlib/service)

SRCS(
    registry_mon_page_ut.cpp
    service_ut.cpp
)

PEERDIR(
    library/cpp/http/simple
    library/cpp/monlib/consumers
    library/cpp/monlib/encode/spack
    library/cpp/monlib/service/pages
)

END()
