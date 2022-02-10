UNITTEST_FOR(library/cpp/tvmauth/client/misc/api/dynamic_dst)

OWNER(g:passport_infra)

SRCS(
    tvm_client_ut.cpp
)

ENV(YA_TEST_SHORTEN_WINE_PATH=1) 
 
END()
