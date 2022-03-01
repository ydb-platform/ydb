
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(ymq-client-cpp)
target_link_libraries(ymq-client-cpp PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-core-protos
  lib-deprecated-client
)
target_sources(ymq-client-cpp PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/ymq/client/cpp/client.cpp
)
