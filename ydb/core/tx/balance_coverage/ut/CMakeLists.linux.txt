
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-core-tx-balance_coverage-ut)
target_include_directories(ydb-core-tx-balance_coverage-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/balance_coverage
)
target_link_libraries(ydb-core-tx-balance_coverage-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  core-tx-balance_coverage
  cpp-testing-unittest
  ydb-core-testlib
)
target_sources(ydb-core-tx-balance_coverage-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/balance_coverage/balance_coverage_builder_ut.cpp
)
add_test(
  NAME
  ydb-core-tx-balance_coverage-ut
  COMMAND
  ydb-core-tx-balance_coverage-ut
)
target_link_flags(ydb-core-tx-balance_coverage-ut
  PUBLIC
  -ldl
  -lrt
  -Wl,--no-as-needed
  -fPIC
  -fPIC
  -lpthread
  -lrt
  -ldl
)
vcs_info(ydb-core-tx-balance_coverage-ut)
