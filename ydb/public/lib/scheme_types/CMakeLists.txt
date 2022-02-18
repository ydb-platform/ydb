add_library(public-lib-scheme_types)
target_link_libraries(public-lib-scheme_types PUBLIC
  contrib-libs-cxxsupp
  yutil
  yql-public-types
)
target_sources(public-lib-scheme_types PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/scheme_types/scheme_type_id.cpp
)
