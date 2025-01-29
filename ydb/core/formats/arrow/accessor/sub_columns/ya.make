LIBRARY()

PEERDIR(
    ydb/core/formats/arrow/accessor/abstract
    ydb/library/formats/arrow
    ydb/library/formats/arrow/protos
    yql/essentials/types/binary_json
)

SRCS(
    GLOBAL constructor.cpp
    GLOBAL request.cpp
    data_extractor.cpp
    accessor.cpp
)

YQL_LAST_ABI_VERSION()

END()
