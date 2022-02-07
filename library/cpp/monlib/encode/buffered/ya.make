LIBRARY()

OWNER(
    g:solomon
    jamel
    msherbakov
)

SRCS(
    buffered_encoder_base.cpp
    string_pool.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/monlib/metrics
)

END()
