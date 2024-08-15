_ydb_sdk_add_library(client-ydb_rate_limiter)

target_link_libraries(client-ydb_rate_limiter PUBLIC
  yutil
  api-grpc
  client-ydb_common_client-impl
  client-ydb_driver
)

target_sources(client-ydb_rate_limiter PRIVATE
  rate_limiter.cpp
)

_ydb_sdk_make_client_component(RateLimiter client-ydb_rate_limiter)
