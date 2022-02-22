add_library(json2_udf INTERFACE)
target_link_libraries(json2_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  ydb-library-binary_json
  yql-minikql-dom
  yql-minikql-jsonpath
)

add_global_library_for(json2_udf.global json2_udf)
target_compile_options(json2_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=21
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(json2_udf.global INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  ydb-library-binary_json
  yql-minikql-dom
  yql-minikql-jsonpath
)
target_sources(json2_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/json2/json2_udf.cpp
)

add_library(json2_udf.udf SHARED)
target_compile_options(json2_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=21
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(json2_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  ydb-library-binary_json
  yql-minikql-dom
  yql-minikql-jsonpath
)
