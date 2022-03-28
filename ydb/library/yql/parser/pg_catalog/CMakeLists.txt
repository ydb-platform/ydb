
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.



add_library(yql-parser-pg_catalog)
target_link_libraries(yql-parser-pg_catalog PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-resource
)
target_sources(yql-parser-pg_catalog PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/yql/parser/pg_catalog/catalog.cpp
)

add_global_library_for(yql-parser-pg_catalog.global yql-parser-pg_catalog)
target_link_libraries(yql-parser-pg_catalog.global PUBLIC
  contrib-libs-cxxsupp
  yutil
  library-cpp-resource
)
target_sources(yql-parser-pg_catalog.global PRIVATE
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/99bdd9e510834dd355c2457fcccc53d5.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/f1a4bc0ed0162412cffb7ad89af053b9.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/1ecd65c896f36a3990d870644a6da9c8.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/46b25697572e7e60703079b71cd18295.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/15e039bc35a86f84476091e328dd74ea.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/ca7bb755fe32f897eb03cf6004d1ad2a.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/f2374a475345069a2bfd38b387dfa11a.cpp
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/462597d51509ecdc14b417ce082dc53b.cpp
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/99bdd9e510834dd355c2457fcccc53d5.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_operator.dat
  KEYS
  pg_operator.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/f1a4bc0ed0162412cffb7ad89af053b9.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_proc.dat
  KEYS
  pg_proc.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/1ecd65c896f36a3990d870644a6da9c8.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_type.dat
  KEYS
  pg_type.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/46b25697572e7e60703079b71cd18295.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_cast.dat
  KEYS
  pg_cast.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/15e039bc35a86f84476091e328dd74ea.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_aggregate.dat
  KEYS
  pg_aggregate.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/ca7bb755fe32f897eb03cf6004d1ad2a.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_opclass.dat
  KEYS
  pg_opclass.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/f2374a475345069a2bfd38b387dfa11a.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_amproc.dat
  KEYS
  pg_amproc.dat
)
resources(yql-parser-pg_catalog.global
  ${CMAKE_BINARY_DIR}/ydb/library/yql/parser/pg_catalog/462597d51509ecdc14b417ce082dc53b.cpp
  INPUTS
  ${CMAKE_SOURCE_DIR}/contrib/libs/postgresql/src/include/catalog/pg_amop.dat
  KEYS
  pg_amop.dat
)
