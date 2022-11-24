
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(client-iam-impl INTERFACE)
target_link_libraries(client-iam-impl INTERFACE
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-client
  cpp-http-simple
  library-cpp-json
  cpp-threading-atomic
  public-lib-jwt
  client-ydb_types-credentials
  client-iam-common
)
