add_library(cpp-actors-protos)
target_link_libraries(cpp-actors-protos PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
)
target_proto_messages(cpp-actors-protos PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/actors.proto
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/interconnect.proto
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/services_common.proto
  ${CMAKE_SOURCE_DIR}/library/cpp/actors/protos/unittests.proto
)
