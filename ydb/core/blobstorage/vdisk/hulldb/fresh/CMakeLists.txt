
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(vdisk-hulldb-fresh)
target_link_libraries(vdisk-hulldb-fresh PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  vdisk-hulldb-base
  blobstorage-vdisk-protos
  ydb-core-protos
)
target_sources(vdisk-hulldb-fresh PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_appendix.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_data.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_datasnap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_segment.cpp
)
