add_library(core-blockstore-core)
target_link_libraries(core-blockstore-core PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
)
target_sources(core-blockstore-core PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/blockstore/core/blockstore.cpp
)
