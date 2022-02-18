add_library(cpp-retry-protos)
target_link_libraries(cpp-retry-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(cpp-retry-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/retry/protos/retry_options.proto
)
target_proto_plugin(cpp-retry-protos
  cpp_styleguide
  cpp_styleguide
)
