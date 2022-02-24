add_library(json_udf INTERFACE)
target_link_libraries(json_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-json-easy_parse
)

add_global_library_for(json_udf.global json_udf)
target_compile_options(json_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(json_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-json-easy_parse
)
target_sources(json_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/json/json_udf.cpp
)

add_library(json_udf.udf SHARED)
target_compile_options(json_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(json_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-json-easy_parse
)
target_sources(json_udf.udf PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/json/json_udf.cpp
)
