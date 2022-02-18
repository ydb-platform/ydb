add_library(library-aclib-protos)
target_link_libraries(library-aclib-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(library-aclib-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/aclib/protos/aclib.proto
)
target_proto_plugin(library-aclib-protos
  cpp_styleguide
  cpp_styleguide
)
