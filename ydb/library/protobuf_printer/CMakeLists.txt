add_library(ydb-library-protobuf_printer)
target_link_libraries(ydb-library-protobuf_printer PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  ydb-library-security
  api-protos-annotations
)
target_sources(ydb-library-protobuf_printer PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/protobuf_printer/hide_field_printer.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/protobuf_printer/stream_helper.cpp
  ${CMAKE_SOURCE_DIR}/ydb/library/protobuf_printer/token_field_printer.cpp
)
