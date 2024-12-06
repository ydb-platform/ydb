LIBRARY()

SRCS(
    csv_arrow.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
)

YQL_LAST_ABI_VERSION()

END()
