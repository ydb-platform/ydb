
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(library-ycloud-api INTERFACE)
target_link_libraries(library-ycloud-api INTERFACE
  contrib-libs-cxxsupp
  yutil
  client-yc_private-iam
  client-yc_private-servicecontrol
  client-yc_private-resourcemanager
  cpp-actors-core
  cpp-grpc-client
  ydb-core-base
  ydb-core-grpc_caching
)
