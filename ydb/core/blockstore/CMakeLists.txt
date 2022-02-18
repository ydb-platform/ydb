add_library(ydb-core-blockstore INTERFACE)
target_link_libraries(ydb-core-blockstore INTERFACE
  contrib-libs-cxxsupp
  yutil
  core-blockstore-core
)
