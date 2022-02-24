add_library(hyperloglog_udf INTERFACE)
target_link_libraries(hyperloglog_udf INTERFACE
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-hyperloglog
)

add_global_library_for(hyperloglog_udf.global hyperloglog_udf)
target_compile_options(hyperloglog_udf.global PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(hyperloglog_udf.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-hyperloglog
)
target_sources(hyperloglog_udf.global PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/hyperloglog/hyperloglog_udf.cpp
)

add_library(hyperloglog_udf.udf SHARED)
target_compile_options(hyperloglog_udf.udf PRIVATE
  -DBUILD_UDF
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(hyperloglog_udf.udf PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
  public-udf-support
  library-cpp-hyperloglog
)
target_sources(hyperloglog_udf.udf PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/hyperloglog/hyperloglog_udf.cpp
)
