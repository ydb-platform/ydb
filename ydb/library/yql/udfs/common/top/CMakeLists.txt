add_library(top_udf INTERFACE)
target_link_libraries(top_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-containers-top_keeper
)

add_global_library_for(top_udf.global top_udf)
target_compile_options(top_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=10
  -DUDF_ABI_VERSION_PATCH=0
)
target_sources(top_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/top/top_udf.cpp
)

add_library(top_udf.udf SHARED)
target_compile_options(top_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=10
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(top_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-containers-top_keeper
)
