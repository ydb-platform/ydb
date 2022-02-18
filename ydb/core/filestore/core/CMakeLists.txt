add_library(core-filestore-core)
target_link_libraries(core-filestore-core PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
)
target_sources(core-filestore-core PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/filestore/core/filestore.cpp
)
