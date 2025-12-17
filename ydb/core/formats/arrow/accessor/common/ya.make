LIBRARY(library-formats-arrow-accessor-common)

PEERDIR(
    contrib/libs/apache/arrow
    ydb/library/actors/core
    yql/essentials/types/binary_json
)

SRCS(
    chunk_data.cpp
    const.cpp
    binary_json_value_view.cpp
)

YQL_LAST_ABI_VERSION()

END()
