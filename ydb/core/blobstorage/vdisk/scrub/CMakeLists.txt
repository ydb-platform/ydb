add_library(blobstorage-vdisk-scrub)
target_link_libraries(blobstorage-vdisk-scrub PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-blobstorage-base
  blobstorage-vdisk-common
)
target_sources(blobstorage-vdisk-scrub PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/blob_recovery.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/blob_recovery_process.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/blob_recovery_queue.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/blob_recovery_request.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/restore_corrupted_blob_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/scrub_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/scrub_actor_huge.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/scrub_actor_mon.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/scrub_actor_pdisk.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/scrub_actor_snapshot.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/scrub_actor_sst.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/scrub/scrub_actor_unreadable.cpp
)
