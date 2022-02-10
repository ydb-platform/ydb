LIBRARY()

OWNER(
    msherbakov
    g:solomon
)

PEERDIR(
    contrib/libs/re2
    library/cpp/json
    library/cpp/monlib/metrics
)

SRCS(
    unistat_decoder.cpp
)

END()
