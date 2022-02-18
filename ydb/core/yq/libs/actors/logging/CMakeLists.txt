add_library(libs-actors-logging INTERFACE)
target_link_libraries(libs-actors-logging INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-protos
)
