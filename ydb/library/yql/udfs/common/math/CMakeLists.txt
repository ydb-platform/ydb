add_library(math_udf INTERFACE)
target_link_libraries(math_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-math-lib
)

add_global_library_for(math_udf.global math_udf)
target_compile_options(math_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=16
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(math_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-math-lib
)
target_sources(math_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/math/math_udf.cpp
)

add_library(math_udf.udf SHARED)
target_compile_options(math_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=16
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(math_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-math-lib
)
