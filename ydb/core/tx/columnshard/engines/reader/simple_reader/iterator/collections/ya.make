LIBRARY()

SRCS(
    abstract.cpp
    not_sorted.cpp
    full_scan_sorted.cpp
    limit_sorted.cpp
)

PEERDIR(
    ydb/core/formats/arrow
)

END()
