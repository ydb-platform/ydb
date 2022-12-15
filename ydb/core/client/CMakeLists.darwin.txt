
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(metadata)
add_subdirectory(minikql_compile)
add_subdirectory(minikql_result_lib)
add_subdirectory(scheme_cache_lib)
add_subdirectory(server)
add_subdirectory(ut)

add_library(ydb-core-client INTERFACE)
target_link_libraries(ydb-core-client INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  ydb-core-base
  core-client-scheme_cache_lib
  core-client-server
  ydb-core-engine
  lib-deprecated-kicli
)
