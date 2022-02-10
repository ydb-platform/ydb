LIBRARY(ticket_parser)

OWNER(
    g:passport_infra
    e-sidorov
    ezaitov
)

PEERDIR(
    contrib/libs/openssl
    library/cpp/openssl/init
)

SRCS(
    keys.cpp
    rw_asn1.c
    rw_key.c
    rw_lib.c
    rw_ossl.c
    rw_pss.c
    rw_pss_sign.c
    rw_sign.c
)

END()

RECURSE_FOR_TESTS(
    ut
    ut_large
)
