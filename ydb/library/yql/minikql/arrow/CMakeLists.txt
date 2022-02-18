add_library(yql-minikql-arrow)
target_compile_options(yql-minikql-arrow PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-minikql-arrow PUBLIC
  contrib-libs-cxxsupp
  yutil
  libs-apache-arrow
  library-yql-minikql
)
target_sources(yql-minikql-arrow PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/minikql/arrow/mkql_memory_pool.cpp
)
