add_library(common-stat-static)
target_compile_options(common-stat-static PRIVATE
  -DUDF_ABI_VERSION_MAJOR=2
  -DUDF_ABI_VERSION_MINOR=9
  -DUDF_ABI_VERSION_PATCH=0
)
target_link_libraries(common-stat-static PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  util-draft
  yql-public-udf
)
target_proto_messages(common-stat-static PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/stat/static/tdigest.proto
)
target_sources(common-stat-static PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/stat/static/static_udf.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/udfs/common/stat/static/tdigest.cpp
)
