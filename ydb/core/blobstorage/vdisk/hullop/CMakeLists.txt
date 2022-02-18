add_library(blobstorage-vdisk-hullop)
target_link_libraries(blobstorage-vdisk-hullop PUBLIC
  contrib-libs-cxxsupp
  yutil
  vdisk-hulldb-barriers
  vdisk-hulldb-base
  vdisk-hulldb-generic
  ydb-core-protos
)
target_sources(blobstorage-vdisk-hullop PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hullop/blobstorage_hull.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hullop/blobstorage_hullactor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllog.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hullop/blobstorage_hulllogcutternotify.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hullop/hullop_compactfreshappendix.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hullop/hullop_entryserialize.cpp
)
