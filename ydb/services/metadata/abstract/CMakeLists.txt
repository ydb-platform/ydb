
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(services-metadata-abstract)
target_link_libraries(services-metadata-abstract PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-library-accessor
  cpp-actors-core
  services-metadata-request
  api-protos
  ydb-core-base
)
target_sources(services-metadata-abstract PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/abstract/common.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/abstract/manager.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/abstract/decoder.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/abstract/fetcher.cpp
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/abstract/kqp_common.cpp
)
