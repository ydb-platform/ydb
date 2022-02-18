add_library(core-blobstorage-storagepoolmon INTERFACE)
target_link_libraries(core-blobstorage-storagepoolmon INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-monlib-dynamic_counters
  ydb-core-base
  core-blobstorage-base
)
