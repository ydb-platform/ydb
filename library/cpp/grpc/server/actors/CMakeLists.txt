add_library(grpc-server-actors)
target_link_libraries(grpc-server-actors PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-actors-core
)
target_sources(grpc-server-actors PRIVATE
  ${CMAKE_SOURCE_DIR}/library/cpp/grpc/server/actors/logger.cpp
)
