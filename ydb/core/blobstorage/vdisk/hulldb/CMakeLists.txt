
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(barriers)
add_subdirectory(base)
add_subdirectory(cache_block)
add_subdirectory(compstrat)
add_subdirectory(fresh)
add_subdirectory(generic)
add_subdirectory(test)

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
)
