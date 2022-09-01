
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

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
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugeheap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/huge/blobstorage_hullhugerecovery.cpp
)
