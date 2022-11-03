
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-kqp-prepare)
target_compile_options(core-kqp-prepare PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-prepare PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-engine
  core-kqp-common
  dq-actors-protos
  yql-dq-type_ann
  public-lib-value
  tools-enum_parser-enum_serialization_runtime
)
target_sources(core-kqp-prepare PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_exec.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_plan.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_type_ann.cpp
)
generate_enum_serilization(core-kqp-prepare
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_plan.h
  INCLUDE_HEADERS
  ydb/core/kqp/prepare/kqp_query_plan.h
)
