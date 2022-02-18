add_library(yql-utils-threading)
target_link_libraries(yql-utils-threading PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(yql-utils-threading PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/utils/threading/async_semaphore.cpp
)
