LIBRARY()

SRCS(
    schema.cpp
    update.cpp
)

PEERDIR(
    ydb/services/bg_tasks/abstract
    contrib/libs/apache/arrow
)

END()
