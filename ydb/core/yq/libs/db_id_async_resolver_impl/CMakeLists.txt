add_library(yq-libs-db_id_async_resolver_impl)
target_compile_options(yq-libs-db_id_async_resolver_impl PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-db_id_async_resolver_impl PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-threading-future
  yq-libs-events
  providers-common-db_id_async_resolver
  providers-dq-actors
)
target_sources(yq-libs-db_id_async_resolver_impl PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/db_id_async_resolver_impl/db_async_resolver_impl.cpp
)
