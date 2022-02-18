add_library(cpp-lwtrace-protos)
target_link_libraries(cpp-lwtrace-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(cpp-lwtrace-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/lwtrace/protos/lwtrace.proto
)
target_proto_plugin(cpp-lwtrace-protos
  cpp_styleguide
  cpp_styleguide
)
