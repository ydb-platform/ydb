UNITTEST_FOR(yql/essentials/core/file_storage)

SIZE(MEDIUM)

SRCS(
    file_storage_ut.cpp
    sized_cache_ut.cpp
    storage_ut.cpp
)

PEERDIR(
    library/cpp/http/server
    library/cpp/threading/future
    library/cpp/deprecated/atomic
    library/cpp/testing/common
    yql/essentials/utils/test_http_server
    yql/essentials/utils/fetch/proto
)

BUILD_ONLY_IF(OS_LINUX)

DEPENDS(
    yql/essentials/core/file_storage/external_utils
)

END()
