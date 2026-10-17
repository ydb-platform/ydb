LIBRARY()

SRCS(
    audit_helper.h
    audit_helper.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/logger
    ydb/core/audit
)

END()
