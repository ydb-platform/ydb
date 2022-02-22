add_library(url_udf INTERFACE)
target_link_libraries(url_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-url_base-lib
)

add_global_library_for(url_udf.global url_udf)
target_compile_options(url_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=23
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(url_udf.global INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-url_base-lib
)
target_sources(url_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/url_base/url_base.cpp
)

add_library(url_udf.udf SHARED)
target_compile_options(url_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=23
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(url_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  common-url_base-lib
)
