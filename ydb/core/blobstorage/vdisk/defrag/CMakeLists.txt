add_library(blobstorage-vdisk-defrag)
target_link_libraries(blobstorage-vdisk-defrag PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  blobstorage-vdisk-common
)
target_sources(blobstorage-vdisk-defrag PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/defrag/defrag_actor.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/defrag/defrag_quantum.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/defrag/defrag_rewriter.cpp
)
