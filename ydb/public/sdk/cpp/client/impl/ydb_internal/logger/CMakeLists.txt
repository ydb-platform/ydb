add_library(impl-ydb_internal-logger)
target_link_libraries(impl-ydb_internal-logger PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-logger
)
target_sources(impl-ydb_internal-logger PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/sdk/cpp/client/impl/ydb_internal/logger/log.cpp
)
