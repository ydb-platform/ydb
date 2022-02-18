add_library(vdisk-hulldb-test)
target_link_libraries(vdisk-hulldb-test PUBLIC
  contrib-libs-cxxsupp
  yutil
  vdisk-hulldb-base
  ydb-core-protos
)
target_sources(vdisk-hulldb-test PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/test/testhull_index.cpp
)
