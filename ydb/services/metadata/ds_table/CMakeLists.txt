
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(services-metadata-ds_table)
target_link_libraries(services-metadata-ds_table PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  ydb-core-base
  core-grpc_services-local_rpc
  core-grpc_services-base
  ydb-core-grpc_services
  services-metadata-initializer
  services-metadata-secret
)
target_sources(services-metadata-ds_table PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/ds_table/accessor_refresh.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/ds_table/accessor_subscribe.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/ds_table/service.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/ds_table/config.cpp
)
