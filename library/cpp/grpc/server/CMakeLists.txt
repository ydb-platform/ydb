add_library(cpp-grpc-server)
target_link_libraries(cpp-grpc-server PUBLIC
  contrib-libs-cxxsupp
  yutil
  tools-enum_parser-enum_serialization_runtime
  contrib-libs-grpc
  monlib-dynamic_counters-percentile
)
target_sources(cpp-grpc-server PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/server/event_callback.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/server/grpc_request.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/server/grpc_server.cpp
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/server/grpc_counters.cpp
)
generate_enum_serilization(cpp-grpc-server
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/server/grpc_request_base.h
  INCLUDE_HEADERS
  library/cpp/grpc/server/grpc_request_base.h
)
