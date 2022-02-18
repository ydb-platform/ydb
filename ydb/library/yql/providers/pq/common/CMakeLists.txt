add_library(providers-pq-common)
target_compile_options(providers-pq-common PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(providers-pq-common PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(providers-pq-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/providers/pq/common/yql_names.cpp
)
