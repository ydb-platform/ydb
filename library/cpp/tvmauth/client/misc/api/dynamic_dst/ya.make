LIBRARY()

OWNER(g:passport_infra)

PEERDIR(
    library/cpp/threading/future
    library/cpp/tvmauth/client
)

SRCS(
    tvm_client.cpp
)

GENERATE_ENUM_SERIALIZATION(tvm_client.h)

END()

RECURSE_FOR_TESTS(
    ut
)
