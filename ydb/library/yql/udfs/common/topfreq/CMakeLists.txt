add_library(topfreq_udf INTERFACE)
target_link_libraries(topfreq_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-topfreq-static
)

add_global_library_for(topfreq_udf.global topfreq_udf)
target_compile_options(topfreq_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=10
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(topfreq_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-topfreq-static
)
target_sources(topfreq_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/topfreq/topfreq_udf.cpp
)

add_library(topfreq_udf.udf SHARED)
target_compile_options(topfreq_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=10
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(topfreq_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-topfreq-static
)
