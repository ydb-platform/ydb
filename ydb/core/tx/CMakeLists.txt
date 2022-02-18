add_library(ydb-core-tx)
target_link_libraries(ydb-core-tx PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-persqueue-config
  ydb-core-protos
  ydb-core-tablet
  ydb-core-tablet_flat
  ydb-core-util
  ydb-library-aclib
)
target_sources(ydb-core-tx PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/tx_proxy_schemereq.cpp
)
