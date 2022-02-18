add_library(ydb-core-scheme_types)
target_link_libraries(ydb-core-scheme_types PUBLIC
  contrib-libs-cxxsupp
  yutil
  public-lib-scheme_types
)
target_sources(ydb-core-scheme_types PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme_types/scheme_type_metadata.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme_types/scheme_type_registry.cpp
  ${CMAKE_SOURCE_DIR}/ydb/core/scheme_types/scheme_types_defs.cpp
)
