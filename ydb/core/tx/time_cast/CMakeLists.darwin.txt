
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

add_library(core-tx-time_cast)
target_link_libraries(core-tx-time_cast PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  ydb-core-protos
  ydb-core-tablet
  ydb-core-tx
)
target_sources(core-tx-time_cast PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/tx/time_cast/time_cast.cpp
)
