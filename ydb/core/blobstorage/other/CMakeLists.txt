add_library(core-blobstorage-other)
target_link_libraries(core-blobstorage-other PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-json-writer
  monlib-service-pages
  cpp-threading-future
  ydb-core-base
  core-blobstorage-base
)
target_sources(core-blobstorage-other PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/other/mon_blob_range_page.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/other/mon_get_blob_page.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/other/mon_vdisk_stream.cpp
)
