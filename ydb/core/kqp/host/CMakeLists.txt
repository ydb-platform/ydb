
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-kqp-host)
target_compile_options(core-kqp-host PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-host PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-kqp-common
  core-kqp-opt
  core-kqp-prepare
  core-kqp-provider
  tx-long_tx_service-public
  yql-core-services
  yql-minikql-invoke_builtins
  library-yql-sql
  library-yql-core
  providers-common-codec
  providers-common-udf_resolve
  yql-providers-config
  providers-result-provider
)
target_sources(core-kqp-host PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_host.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_ne_helper.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_run_data.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_explain_prepared.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_run_physical.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_run_prepared.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_run_scan.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/host/kqp_runner.cpp
)
