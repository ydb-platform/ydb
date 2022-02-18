add_library(core-blobstorage-groupinfo)
target_link_libraries(core-blobstorage-groupinfo PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  cpp-digest-crc32c
  library-cpp-pop_count
  ydb-core-base
  core-base-services
  core-blobstorage-crypto
  blobstorage-vdisk-ingress
  ydb-core-protos
)
target_sources(core-blobstorage-groupinfo PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_blobmap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.cpp
)
