FUZZ()

PEERDIR(
    library/cpp/monlib/encode/prometheus
    library/cpp/monlib/encode/fake
)

SIZE(MEDIUM)

SRCS(
    main.cpp
)

END()
