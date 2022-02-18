add_library(histogram-adaptive-protos)
target_link_libraries(histogram-adaptive-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(histogram-adaptive-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/histogram/adaptive/protos/histo.proto
)
target_proto_plugin(histogram-adaptive-protos
  cpp_styleguide
  cpp_styleguide
)
