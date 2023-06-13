LIBRARY()

SRCS(
    crc.cpp
)

RUN_PROGRAM(
    library/cpp/digest/old_crc/gencrc
    STDOUT crc.inc
)

END()

RECURSE(
    gencrc
    ut
)
