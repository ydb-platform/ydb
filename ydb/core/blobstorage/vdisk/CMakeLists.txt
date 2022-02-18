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
