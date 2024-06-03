G_BENCHMARK()

IF (NOT AUTOCHECK)
    CFLAGS(-DBENCHMARK_ALL_IMPLS)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    library/cpp/case_insensitive_string
    library/cpp/digest/murmur
)

END()
