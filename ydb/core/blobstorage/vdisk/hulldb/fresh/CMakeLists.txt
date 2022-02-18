add_library(vdisk-hulldb-fresh)
target_link_libraries(vdisk-hulldb-fresh PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  vdisk-hulldb-base
  ydb-core-protos
)
target_sources(vdisk-hulldb-fresh PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_appendix.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_data.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_datasnap.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/fresh/fresh_segment.cpp
)
