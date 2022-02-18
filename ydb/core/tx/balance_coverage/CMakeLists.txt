add_library(core-tx-balance_coverage)
target_link_libraries(core-tx-balance_coverage PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
)
target_sources(core-tx-balance_coverage PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/balance_coverage/balance_coverage_builder.cpp
)
