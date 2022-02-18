add_library(set_udf INTERFACE)
target_link_libraries(set_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
)

add_global_library_for(set_udf.global set_udf)
target_compile_options(set_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=19
  -DUDF_ABI_VERSION_PATCH=0
)
target_sources(set_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/set/set_udf.cpp
)

add_library(set_udf.udf SHARED)
target_compile_options(set_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=19
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(set_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
)
