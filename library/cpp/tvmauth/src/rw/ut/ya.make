UNITTEST_FOR(library/cpp/tvmauth/src/rw)

OWNER(
    g:passport_infra
    e-sidorov
    ezaitov
)

SRCS(
    rw_ut.cpp
)

PEERDIR(
    library/cpp/string_utils/base64 
)

END()
