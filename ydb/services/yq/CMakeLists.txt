
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut_integration)

add_library(ydb-services-yq)
target_link_libraries(ydb-services-yq PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-server
  library-cpp-retry
  ydb-core-grpc_services
  core-grpc_services-base
  ydb-library-protobuf_printer
  api-grpc
)
target_sources(ydb-services-yq PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/yq/grpc_service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/yq/private_grpc.cpp
)
