add_executable(ydb-core-tx-sequenceproxy-ut)
target_compile_options(ydb-core-tx-sequenceproxy-ut PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(ydb-core-tx-sequenceproxy-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceproxy
)
target_link_libraries(ydb-core-tx-sequenceproxy-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  core-tx-sequenceproxy
  ydb-core-testlib
)
target_sources(ydb-core-tx-sequenceproxy-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceproxy/sequenceproxy_ut.cpp
)
add_test(
  NAME
  ydb-core-tx-sequenceproxy-ut
  COMMAND
  ydb-core-tx-sequenceproxy-ut
)
target_link_flags(ydb-core-tx-sequenceproxy-ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-core-tx-sequenceproxy-ut)
