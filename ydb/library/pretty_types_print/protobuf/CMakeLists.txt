add_library(library-pretty_types_print-protobuf)
target_link_libraries(library-pretty_types_print-protobuf PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_sources(library-pretty_types_print-protobuf PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/pretty_types_print/protobuf/out.cpp
)
