add_library(library-schlab-protos)
target_link_libraries(library-schlab-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(library-schlab-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/library/schlab/protos/schlab.proto
)
target_proto_plugin(library-schlab-protos
  cpp_styleguide
  cpp_styleguide
)
