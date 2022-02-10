UNITTEST_FOR(ydb/library/yql/dq/actors/compute)

OWNER(
    d-mokhnatkin
    g:yq
    g:yql
)

SRCS(
    dq_compute_issues_buffer_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
)

END()
