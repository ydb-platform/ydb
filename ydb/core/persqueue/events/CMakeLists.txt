add_library(core-persqueue-events)
target_link_libraries(core-persqueue-events PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-keyvalue
  ydb-core-protos
  ydb-core-tablet
  api-protos
)
target_sources(core-persqueue-events PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/events/events.cpp
)
