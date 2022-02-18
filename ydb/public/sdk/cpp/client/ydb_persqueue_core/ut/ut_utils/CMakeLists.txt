add_library(ydb_persqueue_core-ut-ut_utils)
target_compile_options(ydb_persqueue_core-ut-ut_utils PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb_persqueue_core-ut-ut_utils PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  cpp-testing-unittest
  ydb-core-testlib
  library-persqueue-topic_parser_public
  cpp-client-ydb_driver
  cpp-client-ydb_persqueue_core
  cpp-client-ydb_persqueue_public
  cpp-client-ydb_table
)
target_sources(ydb_persqueue_core-ut-ut_utils PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.cpp
)
