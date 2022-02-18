add_library(core-tx-tx_allocator)
target_link_libraries(core-tx-tx_allocator PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-helpers
  cpp-actors-interconnect
  ydb-core-actorlib_impl
  ydb-core-base
  core-engine-minikql
  ydb-core-protos
  ydb-core-tablet
  ydb-core-tablet_flat
)
target_sources(core-tx-tx_allocator PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx_allocator/txallocator__reserve.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx_allocator/txallocator__scheme.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx_allocator/txallocator_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx_allocator/txallocator.cpp
)
