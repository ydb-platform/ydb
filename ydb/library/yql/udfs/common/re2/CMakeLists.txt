add_library(re2_udf INTERFACE)
target_link_libraries(re2_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  contrib-libs-re2
  cpp-deprecated-enum_codegen
)

add_global_library_for(re2_udf.global re2_udf)
target_compile_options(re2_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(re2_udf.global INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  contrib-libs-re2
  cpp-deprecated-enum_codegen
)
target_sources(re2_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/re2/re2_udf.cpp
)

add_library(re2_udf.udf SHARED)
target_compile_options(re2_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(re2_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  contrib-libs-re2
  cpp-deprecated-enum_codegen
)
