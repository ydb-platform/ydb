add_library(vdisk-hulldb-generic)
target_link_libraries(vdisk-hulldb-generic PUBLIC
  contrib-libs-cxxsupp
  yutil
  vdisk-hulldb-base
  ydb-core-protos
)
target_sources(vdisk-hulldb-generic PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hulloptlsn.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idxsnap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sst.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sstslice.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/generic/hullds_sstvec.cpp
)
