add_library(vdisk-hulldb-base)
target_link_libraries(vdisk-hulldb-base PUBLIC
  contrib-libs-cxxsupp
  yutil
  monlib-service-pages
  ydb-core-base
  core-blobstorage-pdisk
)
target_sources(vdisk-hulldb-base PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hulldefs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_hullsatisfactionrank.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.cpp
)
