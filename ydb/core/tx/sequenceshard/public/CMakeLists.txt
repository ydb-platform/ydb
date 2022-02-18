add_library(tx-sequenceshard-public)
target_compile_options(tx-sequenceshard-public PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(tx-sequenceshard-public PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
)
target_sources(tx-sequenceshard-public PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceshard/public/events.cpp
)
