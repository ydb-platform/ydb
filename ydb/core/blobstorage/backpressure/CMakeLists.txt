add_library(core-blobstorage-backpressure)
target_link_libraries(core-blobstorage-backpressure PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  cpp-actors-core
  cpp-containers-intrusive_rb_tree
  ydb-core-base
  core-blobstorage-groupinfo
  core-blobstorage-lwtrace_probes
  ydb-core-protos
)
target_sources(core-blobstorage-backpressure PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/event.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/queue.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/queue_backpressure_client.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/backpressure/unisched.cpp
)
