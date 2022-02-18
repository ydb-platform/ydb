add_library(kqp-opt-peephole)
target_compile_options(kqp-opt-peephole PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(kqp-opt-peephole PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-kqp-common
  ydb-library-naming_conventions
  yql-dq-opt
)
target_sources(kqp-opt-peephole PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/peephole/kqp_opt_peephole_wide_read.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/kqp/opt/peephole/kqp_opt_peephole.cpp
)
