
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(ymq-queues-fifo)
target_link_libraries(ymq-queues-fifo PUBLIC
  contrib-libs-cxxsupp
  yutil
  core-ymq-base
)
target_sources(ymq-queues-fifo PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/queues/fifo/queries.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/queues/fifo/schema.cpp
)
