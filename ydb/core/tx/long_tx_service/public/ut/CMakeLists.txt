add_executable(ydb-core-tx-long_tx_service-public-ut)
target_compile_options(ydb-core-tx-long_tx_service-public-ut PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(ydb-core-tx-long_tx_service-public-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service/public
)
target_link_libraries(ydb-core-tx-long_tx_service-public-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  tx-long_tx_service-public
  cpp-testing-unittest
)
target_sources(ydb-core-tx-long_tx_service-public-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service/public/types_ut.cpp
)
add_test(
  NAME
  ydb-core-tx-long_tx_service-public-ut
  COMMAND
  ydb-core-tx-long_tx_service-public-ut
)
target_link_flags(ydb-core-tx-long_tx_service-public-ut
  PUBLIC
  -ldl
  -lrt
  -Wl,--no-as-needed
  -Wl,-rpath,$ORIGIN
  -fPIC
  -Wl,--gdb-index
  -Wl,-rpath,$ORIGIN
  -fPIC
  -fuse-ld=$(LLD_ROOT-sbr:2283360772)/ld
  -Wl,--no-rosegment
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-core-tx-long_tx_service-public-ut)
