add_library(lib-deprecated-client)
target_link_libraries(lib-deprecated-client PUBLIC
  contrib-libs-cxxsupp
  yutil
  cpp-grpc-client
  library-cpp-messagebus
  public-lib-base
)
target_sources(lib-deprecated-client PRIVATE
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/client/msgbus_client.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/client/msgbus_player.cpp
  ${CMAKE_SOURCE_DIR}/ydb/public/lib/deprecated/client/grpc_client.cpp
)
