LIBRARY()

SRCS(
    ut_helpers.h
    ut_helpers.cpp
)

PEERDIR(
    ydb/core/formats/arrow
    ydb/core/formats/arrow/accessor/sub_columns
    ydb/library/actors/core
    yql/essentials/types/binary_json
)

YQL_LAST_ABI_VERSION()

END()
