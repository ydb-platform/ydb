add_library(core-tx-long_tx_service)
target_compile_options(core-tx-long_tx_service PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-tx-long_tx_service PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-tx-columnshard
  tx-long_tx_service-public
)
target_sources(core-tx-long_tx_service PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service/acquire_snapshot_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service/commit_impl.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service/long_tx_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/long_tx_service/long_tx_service_impl.cpp
)
