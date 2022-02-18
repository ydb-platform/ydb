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
  tools-enum_parser-enum_serialization_runtime
)
target_sources(core-kqp-prepare PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_analyze.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_exec.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_finalize.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_plan.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_rewrite.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_simplify.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_query_substitute.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_type_ann.cpp
)
generate_enum_serilization(core-kqp-prepare
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/prepare/kqp_prepare_impl.h
  INCLUDE_HEADERS
  ydb/core/kqp/prepare/kqp_prepare_impl.h
)
