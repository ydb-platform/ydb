
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-core-kqp-ut-fat)
target_compile_options(ydb-core-kqp-ut-fat PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(ydb-core-kqp-ut-fat PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp
)
target_link_libraries(ydb-core-kqp-ut-fat PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  ydb-core-kqp
  cpp-client-ydb_proto
  core-kqp-counters
  core-kqp-host
  core-kqp-provider
  kqp-ut-common
)
target_sources(ydb-core-kqp-ut-fat PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/ut/fat/kqp_force_newengine_ut.cpp
)
add_test(
  NAME
  ydb-core-kqp-ut-fat
  COMMAND
  ydb-core-kqp-ut-fat
)
target_link_flags(ydb-core-kqp-ut-fat
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
vcs_info(ydb-core-kqp-ut-fat)
