add_library(blobstorage-vdisk-hulldb)
target_link_libraries(blobstorage-vdisk-hulldb PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-blobstorage-base
  blobstorage-vdisk-common
  vdisk-hulldb-barriers
  vdisk-hulldb-base
  vdisk-hulldb-compstrat
  vdisk-hulldb-fresh
  vdisk-hulldb-generic
  blobstorage-vdisk-hullop
  ydb-core-protos
)
target_sources(blobstorage-vdisk-hulldb PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/hulldb_bulksst_add.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/hulldb_bulksstmngr.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/hulldb_recovery.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/hullds_cache_block.cpp
)
