LIBRARY()

NO_WSHADOW()

SRCS(
    counters.cpp
    encode.cpp
    golovan_page.cpp
    page.cpp
)

PEERDIR(
    library/cpp/containers/stack_vector
    library/cpp/monlib/encode/json
    library/cpp/monlib/encode/spack
    library/cpp/monlib/encode/prometheus
    library/cpp/monlib/service/pages
    library/cpp/string_utils/quote
    library/cpp/threading/light_rw_lock
)

END()

RECURSE(
    percentile
    ut
)
