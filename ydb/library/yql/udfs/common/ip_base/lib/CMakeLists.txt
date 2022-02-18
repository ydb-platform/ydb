add_library(common-ip_base-lib)
target_compile_options(common-ip_base-lib PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(common-ip_base-lib PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
)
target_sources(common-ip_base-lib PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/ip_base/lib/ip_base_udf.cpp
)
