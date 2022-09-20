
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(vdisk-hulldb-bulksst_add)
target_link_libraries(vdisk-hulldb-bulksst_add PUBLIC
  contrib-libs-cxxsupp
  yutil
  blobstorage-vdisk-common
  ydb-core-protos
)
target_sources(vdisk-hulldb-bulksst_add PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/bulksst_add/hulldb_bulksst_add.cpp
)
