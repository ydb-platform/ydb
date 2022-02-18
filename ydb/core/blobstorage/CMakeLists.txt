add_library(ydb-core-blobstorage INTERFACE)
target_link_libraries(ydb-core-blobstorage INTERFACE
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-blobstorage-backpressure
  core-blobstorage-base
  core-blobstorage-crypto
  core-blobstorage-dsproxy
  core-blobstorage-groupinfo
  core-blobstorage-incrhuge
  core-blobstorage-lwtrace_probes
  core-blobstorage-nodewarden
  core-blobstorage-other
  core-blobstorage-pdisk
  core-blobstorage-storagepoolmon
  core-blobstorage-testload
  core-blobstorage-vdisk
)
