add_library(common-topfreq-static)
target_compile_options(common-topfreq-static PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=10
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(common-topfreq-static PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-udf
)
target_sources(common-topfreq-static PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/topfreq/static/static_udf.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/topfreq/static/topfreq.cpp
)
