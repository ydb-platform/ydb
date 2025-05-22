LIBRARY()

SRCS(
    spilling_imp.cpp
    spilling_imp.h
    namespaces_list.h
    namespaces_list.cpp
    namespace_cache.h
    namespace_cache.cpp
    interface/spilling.h
    storage/file_storage/file_storage.cpp
    storage/storage.h
    storage/storage.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    yql/essentials/types/binary_json
    yql/essentials/utils
    yql/essentials/utils/log
    ydb/library/yql/core/spilling/storage
)

NO_COMPILER_WARNINGS()

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    storage
)

RECURSE_FOR_TESTS(
    ut
)
