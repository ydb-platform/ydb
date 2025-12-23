LIBRARY()

SRCS(
    printer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/core/formats/arrow/printer
    ydb/library/formats/arrow/protos
    yql/essentials/core/issue/protos
    yql/essentials/public/types
)

YQL_LAST_ABI_VERSION()

END()
