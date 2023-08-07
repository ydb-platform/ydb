LIBRARY()

SRCS(
    url_mapper.cpp
    pattern_group.cpp
    url_preprocessing.cpp
)

PEERDIR(
    ydb/library/yql/providers/common/proto
    ydb/library/yql/utils/log
    library/cpp/regex/pcre
)

END()

