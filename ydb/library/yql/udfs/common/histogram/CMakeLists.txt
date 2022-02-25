add_library(histogram_udf INTERFACE)
target_link_libraries(histogram_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-histogram-adaptive
)

add_global_library_for(histogram_udf.global histogram_udf)
target_compile_options(histogram_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(histogram_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-histogram-adaptive
)
target_sources(histogram_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/histogram/histogram_udf.cpp
)

add_library(histogram_udf.udf SHARED)
target_compile_options(histogram_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(histogram_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-histogram-adaptive
)
target_sources(histogram_udf.udf PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/histogram/histogram_udf.cpp
)
vcs_info(histogram_udf.udf)
