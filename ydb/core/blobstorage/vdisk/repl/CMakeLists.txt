add_library(blobstorage-vdisk-repl)
target_link_libraries(blobstorage-vdisk-repl PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  monlib-service-pages
  ydb-core-base
  core-blobstorage-backpressure
  core-blobstorage-groupinfo
  blobstorage-vdisk-common
  vdisk-hulldb-barriers
  vdisk-hulldb-base
  vdisk-hulldb-generic
  ydb-core-protos
)
target_sources(blobstorage-vdisk-repl PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/repl/blobstorage_hullrepljob.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/repl/blobstorage_replbroker.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/repl/blobstorage_repl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/repl/blobstorage_replproxy.cpp
)
