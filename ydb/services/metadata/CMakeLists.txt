
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(abstract)
add_subdirectory(ds_table)

add_library(ydb-services-metadata)
target_link_libraries(ydb-services-metadata PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  services-metadata-abstract
)
target_sources(ydb-services-metadata PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/services/metadata/service.cpp
)
