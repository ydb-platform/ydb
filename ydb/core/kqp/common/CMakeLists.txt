add_library(core-kqp-common)
target_compile_options(core-kqp-common PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-kqp-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-engine
  core-kqp-expr_nodes
  core-kqp-provider
  ydb-library-aclib
  yql-core-issue
  yql-dq-actors
  yql-dq-common
  tools-enum_parser-enum_serialization_runtime
)
target_sources(core-kqp-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/common/kqp_common.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/common/kqp_resolve.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/common/kqp_ru_calc.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/common/kqp_transform.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/common/kqp_yql.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/common/kqp_timeouts.cpp
)
generate_enum_serilization(core-kqp-common
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/common/kqp_yql.h
  INCLUDE_HEADERS
  ydb/core/kqp/common/kqp_yql.h
)
