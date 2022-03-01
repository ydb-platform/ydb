
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-library-mkql_proto-ut)
target_compile_options(ydb-library-mkql_proto-ut PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(ydb-library-mkql_proto-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/mkql_proto
)
target_link_libraries(ydb-library-mkql_proto-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-malloc-jemalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  ydb-library-mkql_proto
  mkql_proto-ut-helpers
  udf-service-exception_policy
  ydb-core-yql_testlib
)
target_sources(ydb-library-mkql_proto-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/mkql_proto/mkql_proto_ut.cpp
)
add_test(
  NAME
  ydb-library-mkql_proto-ut
  COMMAND
  ydb-library-mkql_proto-ut
)
target_link_flags(ydb-library-mkql_proto-ut
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
vcs_info(ydb-library-mkql_proto-ut)
