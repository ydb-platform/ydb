
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(core-blobstorage-vdisk)
target_link_libraries(core-blobstorage-vdisk PUBLIC
  contrib-libs-cxxsupp
  yutil
  blobstorage-vdisk-anubis_osiris
  blobstorage-vdisk-common
  blobstorage-vdisk-defrag
  blobstorage-vdisk-handoff
  blobstorage-vdisk-huge
  blobstorage-vdisk-hulldb
  blobstorage-vdisk-hullop
  blobstorage-vdisk-ingress
  blobstorage-vdisk-localrecovery
  blobstorage-vdisk-protos
  blobstorage-vdisk-query
  blobstorage-vdisk-repl
  blobstorage-vdisk-scrub
  blobstorage-vdisk-skeleton
  blobstorage-vdisk-syncer
  blobstorage-vdisk-synclog
)
target_sources(core-blobstorage-vdisk PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/vdisk_actor.cpp
)
