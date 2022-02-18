add_library(yql-utils-log)
target_link_libraries(yql-utils-log PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-logger
  cpp-logger-global
)
target_sources(yql-utils-log PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/log/context.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/log/log.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/log/profile.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/log/tls_backend.cpp
)
