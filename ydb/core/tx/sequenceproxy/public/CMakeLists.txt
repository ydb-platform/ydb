add_library(tx-sequenceproxy-public)
target_compile_options(tx-sequenceproxy-public PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(tx-sequenceproxy-public PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-library-aclib
  api-protos
  yql-public-issue
)
target_sources(tx-sequenceproxy-public PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/sequenceproxy/public/events.cpp
)
