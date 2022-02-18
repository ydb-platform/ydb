add_library(ydb-core-filestore INTERFACE)
target_link_libraries(ydb-core-filestore INTERFACE
  contrib-libs-cxxsupp
  yutil
  core-filestore-core
)
