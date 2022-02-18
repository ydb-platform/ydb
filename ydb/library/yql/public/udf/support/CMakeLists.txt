add_library(public-udf-support)
target_compile_options(public-udf-support PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(public-udf-support PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
)
target_sources(public-udf-support PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/public/udf/support/udf_support.cpp
)
