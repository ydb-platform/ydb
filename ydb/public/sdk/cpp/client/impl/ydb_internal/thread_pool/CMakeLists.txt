add_library(impl-ydb_internal-thread_pool)
target_link_libraries(impl-ydb_internal-thread_pool PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(impl-ydb_internal-thread_pool PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_internal/thread_pool/pool.cpp
)
