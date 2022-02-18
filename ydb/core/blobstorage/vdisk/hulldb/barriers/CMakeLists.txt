add_library(vdisk-hulldb-barriers)
target_link_libraries(vdisk-hulldb-barriers PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-blobstorage-base
  vdisk-hulldb-base
  ydb-core-protos
)
target_sources(vdisk-hulldb-barriers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_chain.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_essence.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_public.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_tree.cpp
)
