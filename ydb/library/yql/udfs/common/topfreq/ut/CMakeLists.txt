
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_executable(ydb-library-yql-udfs-common-topfreq-ut)
target_compile_options(ydb-library-yql-udfs-common-topfreq-ut PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_include_directories(ydb-library-yql-udfs-common-topfreq-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/topfreq/static
)
target_link_libraries(ydb-library-yql-udfs-common-topfreq-ut PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-lfalloc
  library-cpp-cpuid_check
  cpp-testing-unittest_main
  common-topfreq-static
  library-yql-minikql
  yql-minikql-comp_nodes
  yql-minikql-computation
  udf-service-exception_policy
)
target_sources(ydb-library-yql-udfs-common-topfreq-ut PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/topfreq/topfreq_udf_ut.cpp
)
add_test(
  NAME
  ydb-library-yql-udfs-common-topfreq-ut
  COMMAND
  ydb-library-yql-udfs-common-topfreq-ut
)
target_link_flags(ydb-library-yql-udfs-common-topfreq-ut
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
vcs_info(ydb-library-yql-udfs-common-topfreq-ut)
