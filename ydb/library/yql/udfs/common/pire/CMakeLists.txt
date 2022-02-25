add_library(pire_udf INTERFACE)
target_link_libraries(pire_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-regex-pire
)

add_global_library_for(pire_udf.global pire_udf)
target_compile_options(pire_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=23
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(pire_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-regex-pire
)
target_sources(pire_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/pire/pire_udf.cpp
)

add_library(pire_udf.udf SHARED)
target_compile_options(pire_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=23
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(pire_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-regex-pire
)
target_sources(pire_udf.udf PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/pire/pire_udf.cpp
)
vcs_info(pire_udf.udf)
