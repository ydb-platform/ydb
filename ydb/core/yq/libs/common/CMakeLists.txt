add_library(yq-libs-common)
target_compile_options(yq-libs-common PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-common PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-dynamic_counters
  libs-control_plane_storage-events
  yq-libs-events
  providers-common-structured_token
  yql-public-issue
  api-protos
)
target_sources(yq-libs-common PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/common/entity_id.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/common/rows_proto_splitter.cpp
)
