
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-blobstorage-testload)
target_link_libraries(core-blobstorage-testload PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-histogram-hdr
  contrib-libs-protobuf
  monlib-dynamic_counters-percentile
  monlib-service-pages
  ydb-core-base
  core-blobstorage-backpressure
  core-blobstorage-base
  core-blobstorage-pdisk
  ydb-core-control
  ydb-core-keyvalue
  ydb-library-workload
  public-lib-base
  public-lib-operation_id
  cpp-client-ydb_proto
)
target_sources(core-blobstorage-testload PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_keyvalue_write.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_memory.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_pdisk_read.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_pdisk_write.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_pdisk_log.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_vdisk_write.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_write.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/testload/test_load_kqp.cpp
)
