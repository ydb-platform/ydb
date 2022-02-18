add_executable(with_offset_ranges_mode_ut)
target_compile_options(with_offset_ranges_mode_ut PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(with_offset_ranges_mode_ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-testing-gmock_in_unittest
  public-lib-json_value
  public-lib-yson_value
  cpp-client-ydb_driver
  ydb_persqueue_core-ut-ut_utils
)
target_sources(with_offset_ranges_mode_ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/common_ut.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/read_session_ut.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/basic_usage_ut.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/compress_executor_ut.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/retry_policy_ut.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils.cpp
)
add_test(
  NAME
  with_offset_ranges_mode_ut
  COMMAND
  with_offset_ranges_mode_ut
)
target_link_flags(with_offset_ranges_mode_ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(with_offset_ranges_mode_ut)
