
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-core-grpc_services-ut)
target_include_directories(ydb-core-grpc_services-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services
)
target_link_libraries(ydb-core-grpc_services-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  ydb-core-grpc_services
  library-cpp-getopt
  cpp-regex-pcre
  library-cpp-svnversion
  core-client-scheme_cache_lib
  ydb-core-testlib
)
target_sources(ydb-core-grpc_services-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/rpc_calls_ut.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/grpc_services/operation_helpers_ut.cpp
)
add_test(
  NAME
  ydb-core-grpc_services-ut
  COMMAND
  ydb-core-grpc_services-ut
)
target_link_flags(ydb-core-grpc_services-ut
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
vcs_info(ydb-core-grpc_services-ut)
