add_library(core-tx-scheme_cache)
target_compile_options(core-tx-scheme_cache PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(core-tx-scheme_cache PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-base
  ydb-core-protos
  ydb-core-scheme
  ydb-library-aclib
  tools-enum_parser-enum_serialization_runtime
)
target_sources(core-tx-scheme_cache PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/scheme_cache/scheme_cache.cpp
)
generate_enum_serilization(core-tx-scheme_cache
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/scheme_cache/scheme_cache.h
  INCLUDE_HEADERS
  ydb/core/tx/scheme_cache/scheme_cache.h
)
