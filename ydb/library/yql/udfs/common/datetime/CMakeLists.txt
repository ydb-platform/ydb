add_library(datetime_udf INTERFACE)
target_link_libraries(datetime_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-timezone_conversion
  util-draft
)

add_global_library_for(datetime_udf.global datetime_udf)
target_compile_options(datetime_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(datetime_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-timezone_conversion
  util-draft
)
target_sources(datetime_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/datetime/datetime_udf.cpp
)

add_library(datetime_udf.udf SHARED)
target_compile_options(datetime_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(datetime_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-timezone_conversion
  util-draft
)
target_sources(datetime_udf.udf PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/datetime/datetime_udf.cpp
)
