
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-core-mind-bscontroller-ut_selfheal)
target_compile_options(ydb-core-mind-bscontroller-ut_selfheal PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(ydb-core-mind-bscontroller-ut_selfheal PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  blobstorage-dsproxy-mock
  blobstorage-pdisk-mock
  core-mind-bscontroller
  core-tx-scheme_board
  udf-service-stub
  yql-sql-pg_dummy
)
target_sources(ydb-core-mind-bscontroller-ut_selfheal PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/mind/bscontroller/ut_selfheal/main.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/mind/bscontroller/ut_selfheal/self_heal_actor_ut.cpp
)
add_test(
  NAME
  ydb-core-mind-bscontroller-ut_selfheal
  COMMAND
  ydb-core-mind-bscontroller-ut_selfheal
)
target_link_flags(ydb-core-mind-bscontroller-ut_selfheal
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
vcs_info(ydb-core-mind-bscontroller-ut_selfheal)
