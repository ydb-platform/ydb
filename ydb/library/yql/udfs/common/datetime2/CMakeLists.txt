add_library(datetime2_udf INTERFACE)
target_link_libraries(datetime2_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  util-draft
  library-yql-minikql
  public-udf-tz
)

add_global_library_for(datetime2_udf.global datetime2_udf)
target_compile_options(datetime2_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=24
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(datetime2_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  util-draft
  library-yql-minikql
  public-udf-tz
)
target_sources(datetime2_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/datetime2/datetime_udf.cpp
)
