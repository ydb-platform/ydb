add_library(yson2_udf INTERFACE)
target_link_libraries(yson2_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-containers-stack_vector
  yson_pull
  yql-minikql-dom
)

add_global_library_for(yson2_udf.global yson2_udf)
target_compile_options(yson2_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=21
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(yson2_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-containers-stack_vector
  yson_pull
  yql-minikql-dom
)
target_sources(yson2_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/yson2/yson2_udf.cpp
)
