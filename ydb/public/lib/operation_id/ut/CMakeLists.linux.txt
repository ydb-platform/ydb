
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-public-lib-operation_id-ut)
target_include_directories(ydb-public-lib-operation_id-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/operation_id
)
target_link_libraries(ydb-public-lib-operation_id-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  public-lib-operation_id
  cpp-testing-unittest
)
target_sources(ydb-public-lib-operation_id-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/operation_id/operation_id_ut.cpp
)
add_test(
  NAME
  ydb-public-lib-operation_id-ut
  COMMAND
  ydb-public-lib-operation_id-ut
)
target_link_flags(ydb-public-lib-operation_id-ut
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
vcs_info(ydb-public-lib-operation_id-ut)
