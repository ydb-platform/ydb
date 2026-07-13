LIBRARY(library-formats-arrow-accessor-common)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/json/writer
    ydb/library/actors/core
    ydb/library/formats/arrow/protos
    yql/essentials/types/binary_json
)

SRCS(
    additional_data.cpp
    chunk_data.cpp
    const.cpp
    binary_json_value_view.cpp
)

YQL_LAST_ABI_VERSION()

END()
