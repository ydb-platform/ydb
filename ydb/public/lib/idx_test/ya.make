LIBRARY()

SRCS(
    idx_test_checker.cpp
    idx_test_common.cpp
    idx_test_data_provider.cpp
    idx_test_stderr_progress_tracker.cpp
    idx_test_loader.cpp
    idx_test_upload.cpp
)

PEERDIR(
    library/cpp/string_utils/base64
    ydb/public/sdk/cpp/client/ydb_table
)

GENERATE_ENUM_SERIALIZATION(idx_test.h)

END()

RECURSE_FOR_TESTS(
    ut
)
