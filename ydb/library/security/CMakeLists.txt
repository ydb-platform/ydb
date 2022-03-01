
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(ydb-library-security)
target_link_libraries(ydb-library-security PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-digest-crc32c
  client-ydb_types-credentials
)
target_sources(ydb-library-security PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/security/util.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/security/ydb_credentials_provider_factory.cpp
)
