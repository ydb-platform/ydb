LIBRARY()

SRCS(
    url_mapper.cpp
    pattern_group.cpp
    url_preprocessing.cpp
)

PEERDIR(
    yql/essentials/core/url_preprocessing/interface
    yql/essentials/providers/common/proto
    yql/essentials/utils/log
    library/cpp/regex/pcre
)

END()

RECURSE(
    interface
)

