
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

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
