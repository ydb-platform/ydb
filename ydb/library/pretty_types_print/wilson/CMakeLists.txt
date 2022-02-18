add_library(library-pretty_types_print-wilson)
target_link_libraries(library-pretty_types_print-wilson PUBLIC
  contrib-libs-cxxsupp
  yutil
  ydb-library-wilson
)
target_sources(library-pretty_types_print-wilson PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/pretty_types_print/wilson/out.cpp
)
