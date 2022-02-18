add_library(blobstorage-vdisk-localrecovery)
target_link_libraries(blobstorage-vdisk-localrecovery PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-blobstorage-pdisk
  ydb-core-protos
)
target_sources(blobstorage-vdisk-localrecovery PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/localrecovery/localrecovery_defs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/localrecovery/localrecovery_logreplay.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/localrecovery/localrecovery_public.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/localrecovery/localrecovery_readbulksst.cpp
)
