add_library(yq-libs-checkpoint_storage)
target_compile_options(yq-libs-checkpoint_storage PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-checkpoint_storage PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-fmt
  cpp-actors-core
  libs-actors-logging
  yq-libs-control_plane_storage
  yq-libs-ydb
  libs-checkpoint_storage-events
  libs-checkpoint_storage-proto
  yq-libs-checkpointing_common
  ydb-library-security
  cpp-client-ydb_scheme
  cpp-client-ydb_table
  dq-actors-compute
  yql-dq-proto
)
target_sources(yq-libs-checkpoint_storage PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpoint_storage/gc.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpoint_storage/storage_proxy.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpoint_storage/storage_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpoint_storage/ydb_checkpoint_storage.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/checkpoint_storage/ydb_state_storage.cpp
)
