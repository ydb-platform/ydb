add_library(yql-core-peephole_opt)
target_compile_options(yql-core-peephole_opt PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-core-peephole_opt PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-core
  yql-core-common_opt
  yql-core-type_ann
)
target_sources(yql-core-peephole_opt PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/peephole_opt/yql_opt_peephole_physical.cpp
)
