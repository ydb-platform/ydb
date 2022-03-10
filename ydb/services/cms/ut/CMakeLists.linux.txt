
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-services-cms-ut)
target_compile_options(ydb-services-cms-ut PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(ydb-services-cms-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/cms
)
target_link_libraries(ydb-services-cms-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  ydb-services-cms
  library-cpp-getopt
  cpp-grpc-client
  cpp-regex-pcre
  library-cpp-svnversion
  ydb-core-testlib
)
target_sources(ydb-services-cms-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/cms/cms_ut.cpp
)
add_test(
  NAME
  ydb-services-cms-ut
  COMMAND
  ydb-services-cms-ut
)
target_link_flags(ydb-services-cms-ut
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
vcs_info(ydb-services-cms-ut)
