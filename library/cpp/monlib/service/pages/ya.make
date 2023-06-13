LIBRARY()

NO_WSHADOW()

SRCS(
    diag_mon_page.cpp
    html_mon_page.cpp
    index_mon_page.cpp
    mon_page.cpp
    pre_mon_page.cpp
    resource_mon_page.cpp
    templates.cpp
    version_mon_page.cpp
    registry_mon_page.cpp
)

PEERDIR(
    library/cpp/build_info
    library/cpp/malloc/api
    library/cpp/svnversion
    library/cpp/resource
    library/cpp/monlib/service
    library/cpp/monlib/encode/json
    library/cpp/monlib/encode/text
    library/cpp/monlib/encode/spack
    library/cpp/monlib/encode/prometheus
)

END()
