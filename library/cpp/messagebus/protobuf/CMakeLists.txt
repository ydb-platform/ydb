add_library(messagebus_protobuf)
target_link_libraries(messagebus_protobuf PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-protobuf
  library-cpp-messagebus
  messagebus_actor
)
target_sources(messagebus_protobuf PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/messagebus/protobuf/ybusbuf.cpp
)
