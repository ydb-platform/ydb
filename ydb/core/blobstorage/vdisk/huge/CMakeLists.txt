add_library(blobstorage-vdisk-huge)
target_link_libraries(blobstorage-vdisk-huge PUBLIC
  contrib-libs-cxxsupp
  yutil
  monlib-service-pages
  ydb-core-base
  blobstorage-vdisk-common
  vdisk-hulldb-base
)
target_sources(blobstorage-vdisk-huge PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhuge.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugedefs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugedelete.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugerecovery.cpp
)
