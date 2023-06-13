LIBRARY()

RUN_PYTHON3(
    gen_tld.py tlds-alpha-by-domain.txt
    IN tlds-alpha-by-domain.txt
    STDOUT tld.inc
)

SRCS(
    tld.cpp
)

PEERDIR(
    library/cpp/digest/lower_case
)

END()

RECURSE_FOR_TESTS(
    ut
)
