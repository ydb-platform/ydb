
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-public-lib-idx_test-ut)
target_include_directories(ydb-public-lib-idx_test-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/idx_test
)
target_link_libraries(ydb-public-lib-idx_test-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  public-lib-idx_test
)
target_sources(ydb-public-lib-idx_test-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/idx_test/ut/idx_test_data_provider_ut.cpp
)
add_test(
  NAME
  ydb-public-lib-idx_test-ut
  COMMAND
  ydb-public-lib-idx_test-ut
)
target_link_flags(ydb-public-lib-idx_test-ut
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
vcs_info(ydb-public-lib-idx_test-ut)
