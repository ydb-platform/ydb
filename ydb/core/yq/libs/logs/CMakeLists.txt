add_library(yq-libs-logs)
target_compile_options(yq-libs-logs PRIVATE
  -DUSE_CURRENT_UDF_ABI_VERSION
)
target_link_libraries(yq-libs-logs PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-protos
  yql-utils-actor_log
)
target_sources(yq-libs-logs PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/yq/libs/logs/log.cpp
)
