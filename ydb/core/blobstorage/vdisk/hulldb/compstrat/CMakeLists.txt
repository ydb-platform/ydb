add_library(vdisk-hulldb-compstrat)
target_link_libraries(vdisk-hulldb-compstrat PUBLIC
  contrib-libs-cxxsupp
  yutil
  vdisk-hulldb-base
  ydb-core-protos
)
target_sources(vdisk-hulldb-compstrat PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_defs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/blobstorage/vdisk/hulldb/compstrat/hulldb_compstrat_selector.cpp
)
