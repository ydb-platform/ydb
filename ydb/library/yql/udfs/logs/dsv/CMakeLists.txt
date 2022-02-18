add_library(dsv_udf INTERFACE)
target_link_libraries(dsv_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-deprecated-split
)

add_global_library_for(dsv_udf.global dsv_udf)
target_compile_options(dsv_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_sources(dsv_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/logs/dsv/dsv_udf.cpp
)

add_library(dsv_udf.udf SHARED)
target_compile_options(dsv_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(dsv_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-deprecated-split
)
