add_library(yql-utils-failure_injector)
target_link_libraries(yql-utils-failure_injector PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-yql-utils
  yql-utils-log
)
target_sources(yql-utils-failure_injector PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/failure_injector/failure_injector.cpp
)
