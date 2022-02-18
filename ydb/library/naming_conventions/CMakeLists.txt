add_library(ydb-library-naming_conventions)
target_link_libraries(ydb-library-naming_conventions PUBLIC
  contrib-libs-cxxsupp
  yutil
)
target_sources(ydb-library-naming_conventions PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/naming_conventions/naming_conventions.cpp
)
