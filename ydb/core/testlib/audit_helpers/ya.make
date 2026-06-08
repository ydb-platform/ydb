LIBRARY()

SRCS(
    audit_helper.h
    audit_helper.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/audit
)

IF (SANITIZER_TYPE == "thread")
    SUPPRESSIONS(tsan.supp)
ENDIF()

END()
