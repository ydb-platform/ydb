
# This file was gererated by the build system used internally in the Yandex monorepo.
# Only simple modifications are allowed (adding source-files to targets, adding simple properties
# like target_include_directories). These modifications will be ported to original
# ya.make files by maintainers. Any complex modifications which can't be ported back to the
# original buildsystem will not be accepted.


add_subdirectory(ut)

add_library(ydb-core-scheme)
target_link_libraries(ydb-core-scheme PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
  library-cpp-charset
  cpp-containers-bitseq
  cpp-deprecated-enum_codegen
  library-cpp-yson
  ydb-core-base
  ydb-core-scheme_types
  ydb-core-util
  ydb-library-aclib
  public-lib-scheme_types
)
target_sources(ydb-core-scheme PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme/scheme_borders.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme/scheme_tablecell.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme/scheme_tabledefs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme/scheme_types_defs.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme/scheme_type_info.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme/scheme_types_proto.cpp
)
