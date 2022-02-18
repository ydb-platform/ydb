add_library(core-tx-tx_allocator_client)
target_link_libraries(core-tx-tx_allocator_client PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
  ydb-core-tablet
  core-tx-tx_allocator
)
target_sources(core-tx-tx_allocator_client PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx_allocator_client/actor_client.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx_allocator_client/client.cpp
)
