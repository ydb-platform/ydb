add_library(kqp-opt-logical)
target_compile_options(kqp-opt-logical PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(kqp-opt-logical PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-kqp-common
  yql-core-extract_predicate
  yql-dq-common
  yql-dq-opt
)
target_sources(kqp-opt-logical PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_effects.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_extract.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_join.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_indexes.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_ranges.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_ranges_predext.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_sqlin.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log_sqlin_compact.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/logical/kqp_opt_log.cpp
)
