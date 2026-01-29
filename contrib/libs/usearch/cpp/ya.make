PROGRAM()

ADDINCL(
    GLOBAL contrib/libs/usearch/include
    GLOBAL contrib/libs/usearch/fp16/include
    GLOBAL contrib/libs/usearch/simsimd/include
)

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    test.cpp
)

END()
