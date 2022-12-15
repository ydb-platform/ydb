
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



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
  library-persqueue-topic_parser
)
target_sources(core-persqueue-events PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/persqueue/events/events.cpp
)
