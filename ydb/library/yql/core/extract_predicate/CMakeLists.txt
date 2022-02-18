add_library(yql-core-extract_predicate)
target_compile_options(yql-core-extract_predicate PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yql-core-extract_predicate PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-core-services
)
target_sources(yql-core-extract_predicate PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/extract_predicate/extract_predicate_dbg.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/core/extract_predicate/extract_predicate_impl.cpp
)
