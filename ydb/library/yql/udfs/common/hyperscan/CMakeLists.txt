add_library(hyperscan_udf INTERFACE)
target_link_libraries(hyperscan_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-regex-hyperscan
  cpp-regex-pcre
)

add_global_library_for(hyperscan_udf.global hyperscan_udf)
target_compile_options(hyperscan_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=23
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(hyperscan_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  cpp-regex-hyperscan
  cpp-regex-pcre
)
target_sources(hyperscan_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/hyperscan/hyperscan_udf.cpp
)
