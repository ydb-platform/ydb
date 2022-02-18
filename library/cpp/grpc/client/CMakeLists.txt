add_library(cpp-grpc-client)
target_link_libraries(cpp-grpc-client PUBLIC
  contrib-libs-cxxsupp
  yutil
  contrib-libs-grpc
)
target_sources(cpp-grpc-client PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/client/grpc_client_low.cpp
)
