add_library(core-engine-minikql)
target_compile_options(core-engine-minikql PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-engine-minikql PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  core-client-minikql_compile
  ydb-core-engine
  ydb-core-formats
  ydb-core-tablet_flat
)
target_sources(core-engine-minikql PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/engine/minikql/flat_local_tx_factory.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/engine/minikql/minikql_engine_host.cpp
)
