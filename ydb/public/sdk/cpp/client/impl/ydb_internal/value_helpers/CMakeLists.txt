add_library(impl-ydb_internal-value_helpers)
target_link_libraries(impl-ydb_internal-value_helpers PUBLIC
  contrib-libs-cxxsupp
  yutil
  api-protos
)
target_sources(impl-ydb_internal-value_helpers PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_internal/value_helpers/helpers.cpp
)
