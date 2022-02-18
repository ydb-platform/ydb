add_executable(ydb-core-tx-long_tx_service-ut)
target_compile_options(ydb-core-tx-long_tx_service-ut PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(ydb-core-tx-long_tx_service-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service
)
target_link_libraries(ydb-core-tx-long_tx_service-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  core-tx-long_tx_service
  ydb-core-testlib
)
target_sources(ydb-core-tx-long_tx_service-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service/long_tx_service_ut.cpp
)
add_test(
  NAME
  ydb-core-tx-long_tx_service-ut
  COMMAND
  ydb-core-tx-long_tx_service-ut
)
target_link_flags(ydb-core-tx-long_tx_service-ut
  PUBLIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-core-tx-long_tx_service-ut)
