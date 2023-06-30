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


# Unittests for url_preprocessing contain what seems to be a sensitive 
# information about Yandex internals, so let's not export them for now.
IF (NOT EXPORT_CMAKE)
    RECURSE(
        ut
    )
ENDIF()
