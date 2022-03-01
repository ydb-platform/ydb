
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-public-sdk-cpp-client-draft-ut)
target_include_directories(ydb-public-sdk-cpp-client-draft-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/draft
)
target_link_libraries(ydb-public-sdk-cpp-client-draft-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  cpp-client-draft
)
target_sources(ydb-public-sdk-cpp-client-draft-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/draft/ydb_scripting_response_headers_ut.cpp
)
add_test(
  NAME
  ydb-public-sdk-cpp-client-draft-ut
  COMMAND
  ydb-public-sdk-cpp-client-draft-ut
)
target_link_flags(ydb-public-sdk-cpp-client-draft-ut
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
vcs_info(ydb-public-sdk-cpp-client-draft-ut)
