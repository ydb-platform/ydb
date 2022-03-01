
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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
