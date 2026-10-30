Y_BENCHMARK()

TAG(ya:not_autocheck)


SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/containers/comptrie
    library/cpp/containers/dense_hash
    library/cpp/on_disk/aho_corasick
)

END()
