FUZZ()

FUZZ_OPTS(-rss_limit_mb=1024)

SIZE(MEDIUM)

PEERDIR(
    library/cpp/monlib/encode/spack
    library/cpp/monlib/encode/fake
)

SRCS(
    main.cpp
)

END()
