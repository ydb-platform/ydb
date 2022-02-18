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
)
